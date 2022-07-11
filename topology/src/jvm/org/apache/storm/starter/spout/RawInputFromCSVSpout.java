package org.apache.storm.starter.spout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class RawInputFromCSVSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RawInputFromCSVSpout.class);

    private File csv;
    private List<String> outputFields;
    private BufferedReader br;
    private SpoutOutputCollector collector;

    public RawInputFromCSVSpout(File rawInputCsv, List<String> outputFields) throws FileNotFoundException {
        Objects.requireNonNull(rawInputCsv);
        Objects.requireNonNull(outputFields);

        this.csv = rawInputCsv;
        this.outputFields = outputFields; 
        
    }

    public static RawInputFromCSVSpout newInstance(File csv) throws IOException {
        List<String> outputFields;
        try (BufferedReader br = newReader(csv)) {
            String header = br.readLine();
            LOG.debug("Header: {}", header);
            header = header.replaceAll("\"", "");
            LOG.debug("Processed header: {}", header);
            final String[] inputNames = header.split(",");
            outputFields = Arrays.asList(inputNames);
        }
        return new RawInputFromCSVSpout(csv, outputFields);
    }

    private static BufferedReader newReader(File csv) throws FileNotFoundException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(csv)));
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        openReader();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(5);
        try {
            String line = null;
            
            while ((line = br.readLine()) != null) {
                collector.emit(Arrays.asList(line.split(",")));

                System.out.println(String.format("TEST {}", line));
            }
        } catch (IOException e) {
            closeReader();
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(outputFields));
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void close() {
        closeReader();
    }

    // =====

    private void openReader() {
        try {
            br = newReader(csv);
            br.readLine();          // disregard first line because it has header, already read
        } catch (IOException e) {
            closeReader();
            throw new RuntimeException(e);
        }
    }

    private void closeReader() {
        if (br != null) {
            try {
                br.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}