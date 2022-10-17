/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package diarsid.jdbc.impl.sqlhistory;

import java.time.temporal.Temporal;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import diarsid.support.objects.collections.StreamsSupport;
import diarsid.support.strings.StringUtils;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 *
 * @author Diarsid
 */
public class SqlHistoryReportMaker {
    
    private static final String LINE_SEPARATOR;
    
    static {
        LINE_SEPARATOR = System.lineSeparator();    
    }
    
    private final StringBuilder historyBuilder;
    private final boolean replaceParams;
    private int counter;
    
    public SqlHistoryReportMaker(boolean replaceParams, UUID transactionUuid) {
        this.historyBuilder = new StringBuilder();
        this.replaceParams = replaceParams;
        this.init(transactionUuid);
    }
    
    private void init(UUID transactionUuid) {
        this.counter = 0;
        this.historyBuilder
                .append(LINE_SEPARATOR)
                .append("[SQL HISTORY] transaction: " + transactionUuid)
                .append(LINE_SEPARATOR);
    }

    public void setCounter(int number) {
        this.counter = number;
    }
    
    private void addCounterAndMillis(long millis) {
        this.historyBuilder
                .append(format("[%d] - %d ms", this.counter, millis))
                .append(LINE_SEPARATOR);
        this.counter++;
    }

    private void addCounter() {
        this.historyBuilder
                .append(format("[%d] ", this.counter))
                .append(LINE_SEPARATOR);
        this.counter++;
    }
    
    public void add(String sql, long millis) {
        this.addCounterAndMillis(millis);
        this.historyBuilder
                .append(sql)
                .append(LINE_SEPARATOR);
    }

    public void addComment(String comment, long millis) {
        this.addCounterAndMillis(millis);
        this.historyBuilder
                .append(comment)
                .append(LINE_SEPARATOR);
    }

    public void add(Throwable e) {
        this.addCounter();
        this.historyBuilder
                .append("[THROWABLE] ")
                .append(e.getClass().getCanonicalName())
                .append(" : ")
                .append(e.getMessage())
                .append(LINE_SEPARATOR);
    }

    public void add(String sql, long millis, boolean isBatch, Object... params) {
        this.addCounterAndMillis(millis);
        if ( this.replaceParams && ! isBatch ) {
            int length = this.historyBuilder.length();

            List<String> args = stream(params)
                    .flatMap(StreamsSupport.unwrapping())
                    .map(SqlHistoryReportMaker::stringify)
                    .collect(toList());

            this.historyBuilder
                    .append(sql)
                    .append(LINE_SEPARATOR);

            StringUtils.replaceAllWith("?", this.historyBuilder, length, args, false);
        }
        else {
            if ( isBatch ) {
                this.historyBuilder
                        .append("[batch] size: ").append(params.length)
                        .append(LINE_SEPARATOR);
                this.historyBuilder
                        .append(sql)
                        .append(LINE_SEPARATOR);
                this.addBatchParamsLines(params);
            }
            else {
                this.historyBuilder
                        .append(sql)
                        .append(LINE_SEPARATOR);
                this.addParamsLine(params);
            }
        }
    }
    
    public void add(String sql, long millis, boolean isBatch, List<? extends Object> params) {
        this.addCounterAndMillis(millis);
        if ( this.replaceParams && ! isBatch ) {
            int length = this.historyBuilder.length();

            List<String> args = params
                    .stream()
                    .flatMap(StreamsSupport.unwrapping())
                    .map(SqlHistoryReportMaker::stringify)
                    .collect(toList());

            this.historyBuilder
                    .append(sql)
                    .append(LINE_SEPARATOR);

            StringUtils.replaceAllWith("?", this.historyBuilder, length, args, false);
        }
        else {
            if ( isBatch ) {
                this.historyBuilder
                        .append("[batch] size: ").append(params.size())
                        .append(LINE_SEPARATOR);
                this.historyBuilder
                        .append(sql)
                        .append(LINE_SEPARATOR);
                this.addBatchParamsLines(params);
            }
            else {
                this.historyBuilder
                        .append(sql)
                        .append(LINE_SEPARATOR);
                this.addParamsLine(params);
            }
        }
    }

    private void addParamsLine(Object... params) {
        this.historyBuilder
                .append("( ")
                .append(stream(params)
                        .map(obj -> stringify(obj))
                        .collect(joining(", ")))
                .append(" )")
                .append(LINE_SEPARATOR);
    }

    private void addParamsLine(List<? extends Object> params) {
        this.historyBuilder
                .append("( ")
                .append(params
                        .stream()
                        .map(obj -> stringify(obj))
                        .collect(joining(", ")))
                .append(" )")
                .append(LINE_SEPARATOR);
    }

    private void addBatchParamsLines(Object... params) {
        AtomicInteger argsSubsetCounter = new AtomicInteger(0);
        this.historyBuilder
                .append("( ")
                .append(LINE_SEPARATOR)
                .append(stream(params)
                        .map(obj -> " [" + argsSubsetCounter.getAndIncrement() + "] " + stringify(obj) + " ")
                        .collect(joining(LINE_SEPARATOR)))
                .append(LINE_SEPARATOR)
                .append(" )")
                .append(LINE_SEPARATOR);
    }

    private void addBatchParamsLines(List<? extends Object> params) {
        AtomicInteger argsSubsetCounter = new AtomicInteger(0);
        this.historyBuilder
                .append("( ")
                .append(LINE_SEPARATOR)
                .append(params
                        .stream()
                        .map(obj -> " [" + argsSubsetCounter.getAndIncrement() + "] " + stringify(obj) + " ")
                        .collect(joining(LINE_SEPARATOR)))
                .append(LINE_SEPARATOR)
                .append(" )")
                .append(LINE_SEPARATOR);
    }

    private static String stringify(Object obj) {
        if ( obj instanceof Enum ) {
            return "'" + ((Enum) obj).name() + "'";
        }
        else if ( obj instanceof String ) {
            return "'" + obj + "'";
        }
        else if ( obj instanceof Character ) {
            return "'" + obj + "'";
        }
        else if ( obj instanceof UUID ) {
            return "'" + obj + "'";
        }
        else if ( obj instanceof Temporal) {
            return "'" + obj + "'";
        }
        else if ( obj instanceof byte[] || obj instanceof Byte[] ) {
            return format("bytes:%s", ((byte[]) obj).length );
        }
        else if ( obj instanceof Collection ) {
            return stringifyAsCollection(obj);
        }
        else if ( obj.getClass().isArray() ) {
            return stringifyAsArray(obj);
        }
        else {
            return obj.toString();
        }
    }

    private static String stringifyAsCollection(Object obj) {
        return ((Collection<Object>) obj)
                .stream()
                .map(SqlHistoryReportMaker::stringify)
                .collect(joining(", "));
    }

    private static String stringifyAsArray(Object obj) {
        return stream(((Object[]) obj))
                .map(SqlHistoryReportMaker::stringify)
                .collect(joining(", "));
    }
    
    public void add(String sql, long millis, List<List> batchParams) {
        this.addCounterAndMillis(millis);
        this.historyBuilder
                .append(sql)
                .append(LINE_SEPARATOR);

        batchParams.forEach(this::addParamsLine);
    }
    
    public String makeReport() {
        return this.historyBuilder.toString();
    }
    
    public void rollback() {
        this.historyBuilder
                .append("[ROLLBACK]")
                .append(LINE_SEPARATOR);
    }
    
    public void clear() {
        this.historyBuilder.delete(0, this.historyBuilder.length());
    }
}
