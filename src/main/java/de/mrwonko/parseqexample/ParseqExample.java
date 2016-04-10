package de.mrwonko.parseqexample;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.parseq.Task;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.trace.TraceUtil;

public class ParseqExample {
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

            naiveCountdown( blockingExecutor, 5, TimeUnit.SECONDS )
                .andThen( "say world", __ -> { System.out.println( "world" ); } )
                .withTimeout( 1, TimeUnit.SECONDS )
                .recover( "recover from timeout", err -> null ),

            asyncCountdown( scheduler, 1, TimeUnit.SECONDS )
                .andThen( "say hello", __ -> { System.out.println( "hello" ); } ) )
        // and what to do with the results (mostly ignore them)
        .map( "print result", ( repos, __, ___  ) -> {
            System.out.println( "Got the following freiheit-com GitHub repos:" );
            for ( final String repoName : repos ) {
                System.out.println( "- " + repoName );
            }
            return repos;
        } );

        task.setTraceValueSerializer( Joiner.on( ", " )::join );

        // submit the task for execution
        engine.run(
            task
            // sadly there's no Task.finally(), but this works similarly
            .transform( "finally", maybeResult -> {
                // always clean up after yourself - after all, this isn't C++ where you have a nicely defined object lifetime and destructors
                httpClient.close();

                scheduler.shutdown();
                blockingExecutor.shutdown();
                executor.shutdown();

                System.out.println( TraceUtil.getJsonTrace( task ) );
                return maybeResult;
            } ) );
        engine.shutdown();
    }
    
    private static Task<? extends List<String>> getFreiheitRepos(
            final HttpAsyncClient client ) {
        final HttpGet request = new HttpGet( "https://api.github.com/orgs/freiheit-com/repos" );
        request.addHeader( "Accept", "application/vnd.github.v3+json" );

        return execute( client, request )
        .map( "read content", ParseqExample::readContent )
        .map( "parse content", ( final String content ) -> {
            final ObjectMapper mapper = new ObjectMapper();
            final ArrayNode repoList = mapper.readValue( content, ArrayNode.class );
            final ImmutableList.Builder<String> builder = ImmutableList.builder();
            for ( final JsonNode repo : repoList ) {
                builder.add( repo.path( "name" ).getTextValue() );
            }
            return builder.build();
        } )
        .recover( "recover from failed request", err -> {
            err.printStackTrace( System.err );
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

    private static Task<Void> naiveCountdown(
            final Executor executor,
            final long time,
            final TimeUnit unit ) {
        return Task.blocking( "naive countdown " + time + " " + unit, () -> {
            Thread.sleep( unit.toMillis( time ) );
            System.out.println( "naive countdown " + time + " " + unit + " completed." );
            return null;
        }, executor );
    }

    private static Task<Void> asyncCountdown(
            final ScheduledExecutorService scheduler,
            final long time,
            final TimeUnit unit ) {
        return Task.async( "async countdown " + time + " " + unit, () -> {
            final SettablePromise<Void> promise = Promises.settable();
            scheduler.schedule( () -> {
                    promise.done( null );
                    System.out.println( "async countdown " + time + " " + unit + " completed." );
                }, time, unit );
            return promise;
        } );
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
}
