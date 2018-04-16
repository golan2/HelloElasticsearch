package com.mortix.log.proxy;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class InsertToEs {

    private static final String LOGLEVEL    = "DEBUG";
    private static final String PROJECT     = "b";
    private static final String ENVIRONMENT = "B";
    private static final int    SEED_SECOND = 2;

    private static final String           START_DATE    = String.format("1963-02-03T00:00:%2d,000", SEED_SECOND);
    private static final SimpleDateFormat DF_INDEX_NAME = new SimpleDateFormat("yyyy.MM.dd"               );
    private static final SimpleDateFormat DF_INPUT      = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss,SSS");
    private static final int              SIZE          = 5;
    private static final String           CLUSTER_NAME  = "elasticsearch";



    public static void main(String[] args) throws UnknownHostException, ParseException {

        Settings settings = Settings.builder().put("cluster.name", CLUSTER_NAME).build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));


        final Date date = DF_INPUT.parse(START_DATE);
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        for (int i = 0; i < SIZE; i++) {
            cal.add(Calendar.MILLISECOND, 1);
            String json = "{\"@timestamp\":\""+ DF_INPUT.format(cal.getTime())+"Z\",\"time\":\""+ DF_INPUT.format(cal.getTime())+"000000Z\",\"DataFlow project\":\""+ PROJECT +"\",\"DataFlow environment\":\""+ ENVIRONMENT +"\",\"DataFlow organization\":\"load-test-org\",\"DataFlow component\":\"\",\"loglevel\":\""+ LOGLEVEL +"\",\"Message\":\""+String.format("Document not in DEBUG %4d", i)+"\",\"host\":\"filebeat-5mgw5\",\"prospector\":{\"type\":\"log\"},\"datacenter\":{\"name\":\"dataflow-load-westus\"},\"source\":\"/hostfs/var/lib/docker/containers/b9feecd7ecb19dbc3032281b4d2c9147e04c46b71c5d936f9190170cbe0b86db/b9feecd7ecb19dbc3032281b4d2c9147e04c46b71c5d936f9190170cbe0b86db-json.log\",\"offset\":703583,\"unique originating id\":\"5d2d82e0-a472-4ec7-a791-ecf9220e9d63\",\"tags\":[\"beats_input_raw_event\"],\"identity of the caller\":\"middleware:_default\",\"stream\":\"stdout\",\"beat\":{\"name\":\"filebeat-5mgw5\",\"hostname\":\"filebeat-5mgw5\",\"version\":\"6.0.0\"},\"@version\":\"1\",\"customer facing\":\"N\"}";
            IndexResponse response = client.prepareIndex(getIndexNameForDate(cal), "_doc")
                    .setSource(json, XContentType.JSON)
                    .get();
            System.out.println("["+i+"] response.status()="+response.status());
        }

        client.close();

    }

    private static String getIndexNameForDate(Calendar cal) {
        return "filebeat-" + DF_INDEX_NAME.format(cal.getTime());
    }

}
