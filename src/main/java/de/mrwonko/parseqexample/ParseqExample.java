package de.mrwonko.parseqexample;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.client.HttpAsyncClient;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.parseq.Task;
import com.linkedin.parseq.Tasks;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.trace.TraceUtil;

public class ParseqExample {
    private static final Logger LOG = LoggerFactory.getLogger( ParseqExample.class );

    private static final boolean TRACE = false;
    private static final boolean LEFT_PAD = false;

    public static void main( final String[] args ) {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final ExecutorService blockingExecutor = Executors.newCachedThreadPool();
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        // Tasks are executed by an engine.
        final Engine engine = new EngineBuilder()
            .setTaskExecutor( executor )
            .setTimerScheduler( scheduler )
            .build();

        CloseableHttpAsyncClient httpClient = HttpAsyncClients.createDefault();
        httpClient.start();

        // Define some tasks to be executed in parallel
        final Task<List<String>> task = Task.par(
            getFreiheitRepos( httpClient ),

            asyncWait( scheduler, 1, TimeUnit.SECONDS )
                .andThen( "say hello", __ -> { LOG.info( "hello" ); } ),

            naiveWait( blockingExecutor, 5, TimeUnit.SECONDS )
                .andThen( "say world", __ -> { LOG.info( "world" ); } )
                .withTimeout( 1, TimeUnit.SECONDS )
                .recover( "recover from timeout", err -> null ) )

        // and what to do with the results (mostly ignore them)
        .map( "print result", ( repos, __, ___ ) -> {
            LOG.info( "Got the following freiheit-com GitHub repos:" );
            for ( final String repoName : repos ) {
                LOG.info( repoName );
            }
            return repos;
        } );

        task.setTraceValueSerializer( Joiner.on( ", " )::join );

        // submit the task for execution
        LOG.info( "Beginning execution." );
        engine.run(
            task
            // sadly there's no Task.finally(), but this works similarly
            .transform( "finally", maybeResult -> {
                // always clean up after yourself - after all, this isn't C++ where you have a nicely defined object lifetime and destructors
                httpClient.close();

                scheduler.shutdown();
                blockingExecutor.shutdown();
                executor.shutdown();

                if ( TRACE ) {
                    System.out.println( TraceUtil.getJsonTrace( task ) );
                }
                return maybeResult;
            } ) );
        engine.shutdown();
    }
    
    private static Task<? extends List<String>> getFreiheitRepos(
            final HttpAsyncClient client ) {
        final HttpGet request = new HttpGet( "https://api.github.com/orgs/freiheit-com/repos" );
        request.addHeader( "Accept", "application/vnd.github.v3+json" );

        Task<List<String>> fetchTask = execute( client, request )
            .map( "read content", ParseqExample::readContent )
            .map( "parse content", ( final String content ) -> {
                // JSON-Parsing using Jackson
                final ObjectMapper mapper = new ObjectMapper();
                final ArrayNode repoList = mapper.readValue( content, ArrayNode.class );
                final ImmutableList.Builder<String> builder = ImmutableList.builder();
                for ( final JsonNode repo : repoList ) {
                    builder.add( repo.path( "name" ).getTextValue() );
                }
                return builder.build();
            } );
        if ( LEFT_PAD ) {
            fetchTask = fetchTask.flatMap( repos -> leftPadAll( client, repos, 30 ) );
        }
        return fetchTask
            .recover( "recover from failed request", err -> {
                LOG.error( "failed to request Freiheit repos", err );
                return ImmutableList.of();
            } );
    }

    private static Task<HttpResponse> execute(
            final HttpAsyncClient client,
            final HttpUriRequest request ) {
        final Task<HttpResponse> task = Task.async(
            "Http Request: " + request.getRequestLine(),
            () -> {
                final SettablePromise<HttpResponse> promise = Promises.settable();
                client.execute( request, new FutureCallback<HttpResponse>() {
                    @Override
                    public void completed( final HttpResponse result ) {
                        promise.done( result );
                    }

                    @Override
                    public void failed( final Exception ex ) {
                        promise.fail( ex );
                    }

                    @Override
                    public void cancelled() {
                        promise.fail( new CancellationException() );
                    }
                } );
                return promise;
            } );
        return task;
    }

    private static String readContent(
            final HttpResponse response )
            throws Exception {
        final int status = response.getStatusLine().getStatusCode();
        if ( status / 100 != 2 ) {
            throw new RuntimeException( "Got status " + status );
        }
        try (
                final InputStream is = response.getEntity().getContent();
                final Scanner scanner = new Scanner( is ) ) {
            scanner.useDelimiter( "\\A" );
            return scanner.hasNext() ? scanner.next() : "";
        }
    }

    private static Task<Void> naiveWait(
            final Executor executor,
            final long time,
            final TimeUnit unit ) {
        return Task.blocking( "naive wait " + time + " " + unit, () -> {
            Thread.sleep( unit.toMillis( time ) );
            LOG.info( "naive wait " + time + " " + unit + " completed." );
            return null;
        }, executor );
    }

    private static Task<Void> asyncWait(
            final ScheduledExecutorService scheduler,
            final long time,
            final TimeUnit unit ) {
        return Task.async( "async wait " + time + " " + unit, () -> {
            final SettablePromise<Void> promise = Promises.settable();
            scheduler.schedule( () -> {
                    promise.done( null );
                    LOG.info( "async wait " + time + " " + unit + " completed." );
                }, time, unit );
            return promise;
        } );
    }

    private static Task<List<String>> leftPadAll(
            final HttpAsyncClient client,
            final List<String> strs,
            final int len ) {
        final List<Task<String>> tasks = Lists.transform( strs, str -> leftPad( client, str, len ) );
        return Tasks.par( tasks );
    }

    private static Task<String> leftPad(
            final HttpAsyncClient client,
            final String str,
            final int len ) {
        final HttpGet request;
        try {
            request = new HttpGet( "https://api.left-pad.io/"
                + "?len=" + len
                + "&str=" + URLEncoder.encode( str, "UTF-8" ) );
        } catch ( final UnsupportedEncodingException e ) {
            return Task.failure( e );
        }
        return execute( client, request )
                .map( "read content", ParseqExample::readContent )
                .map( "parse content", content -> new ObjectMapper()
                        .readValue( content, ObjectNode.class )
                        .get( "str" )
                        .getTextValue() );
    }
}
