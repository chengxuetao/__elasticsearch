package com.example.elasticsearch;

import java.util.concurrent.TimeUnit;

import com.example.elasticsearch.utils.BaseTest;

public class DeleteIndicesTest extends BaseTest {

    private String destIndexName;

    public static void main(String[] args) throws Exception {
        try {
            init(null);
            DeleteIndicesTest test = new DeleteIndicesTest();
            test.prepare();
            test.execute();

            System.out.println("execute end...");
            TimeUnit.SECONDS.sleep(5);
            System.out.println("main end...");
        } finally {
            if (factory != null)
                factory.destroy();
        }
    }

    public void prepare() {
        destIndexName = "dbf0927b5cb44598890f4c6857da1240";
    }

    public void execute() {
        try {
            if (esOperation.indexExists(destIndexName + "*")) {
                esOperation.deleteIndices(destIndexName + "*");
            }
            if (esOperation.templateExists(destIndexName + "*")) {
                esOperation.deleteTemplate(destIndexName + "*");
            }
        } catch (Throwable e) {
            System.out.println("clear datamodel temporary table error. indexName=" + destIndexName);
            e.printStackTrace();
        }
    }

}
