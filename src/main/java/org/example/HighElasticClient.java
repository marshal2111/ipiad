package org.example;

import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.ml.job.results.Bucket;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.adjacency.ParsedAdjacencyMatrix;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders.*;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class HighElasticClient {
    RestHighLevelClient client;
    String IndexName = "meows";


    public void Close() throws IOException {
        client.close();
    }

    public void NewClient() throws IOException {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        createIndex(IndexName);
    }

    private void createIndex(String indexName) {
        try {
            // Проверить, существует ли индекс
            boolean indexExists = client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            if (indexExists) {
                System.out.println("Index already exists: " + indexName);
                return;
            }

            // Создать индекс
            CreateIndexRequest request = new CreateIndexRequest(indexName);

            request.mapping("{\n" +
                    "      \"properties\": {\n" +
                    "        \"date\": {\n" +
                    "          \"type\": \"date\",\n" +
                    "          \"format\": \"dd.MM.yyyy\"\n" +
                    "        }\n" +
                    "  }\n" +
                    "}", XContentType.JSON);
            client.indices().create(request, RequestOptions.DEFAULT);
            System.out.println("Index created: " + indexName);
        } catch (IOException e) {
            System.out.println("ERROR creating index: " + e.getMessage());
            return;
        }
    }



//    public boolean storeNewsInfo(NewsInfo newsInfo) throws IOException {
//        IndexRequest request = new IndexRequest(IndexName);
//        request.id(newsInfo.getHash());
//        request.source(newsInfo.toMap(), XContentType.JSON);
//
//        try {
//            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
//            System.out.println("Indexed with id " + response.getId());
//            return true;
//        } catch (IOException e) {
//            System.out.println("ERROR storing news info: " + e.getMessage());
//            return false;
//        }
//    }


    public boolean storeNewsInfo(NewsInfo newsInfo) throws IOException {
        IndexRequest request = new IndexRequest(IndexName);
        request.id(newsInfo.getHash());

        // Установить формат даты
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");
        String formattedDate = dateFormat.format(newsInfo.getDate());

        // Создать карту для хранения источника документа
        Map<String, Object> source = new HashMap<>();
        source.putAll(newsInfo.toMap());
        source.put("date", formattedDate);
        System.out.println(source);
        // Установить источник документа
        request.source(source, XContentType.JSON);

        try {
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            System.out.println("Indexed with id " + response.getId());
            return true;
        } catch (IOException e) {
            System.out.println("ERROR storing news info: " + e.getMessage());
            return false;
        }
    }


    public NewsInfo searchNewsInfo(String hash) throws IOException {
        GetRequest request = new GetRequest(IndexName, hash);

        try {
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            if (response.isExists()) {
                Map<String, Object> sourceAsMap = response.getSourceAsMap();
                NewsInfo newsInfo = new NewsInfo();
                newsInfo.fromMap(sourceAsMap);
                System.out.println("Found news " + newsInfo.getHeader());
                return newsInfo;
            } else {
                System.out.println("News not found");
                return null;
            }
        } catch (IOException e) {
            System.out.println("ERROR searching news by id: " + e.getMessage());
            return null;
        }
    }

    public NewsInfo searchNewsInfoAnd(String title, String link) throws IOException {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(QueryBuilders.matchQuery("header", title));
        queryBuilder.must(QueryBuilders.matchQuery("link", link));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);

        SearchRequest request = new SearchRequest(IndexName);
        request.source(sourceBuilder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            TotalHits total = response.getHits().getTotalHits();
            if (total == null) {
                System.out.println("total is null");
                return null;
            }

            System.out.println("There are " + total.value + " results");

            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                NewsInfo newsInfo = new NewsInfo();
                newsInfo.fromMap(sourceAsMap);
                if (newsInfo != null) {
                    System.out.println("Found news " + newsInfo.getHeader() + ", score " + hit.getScore());
                    return newsInfo;
                } else {
                    System.out.println("ni is null");
                    break;
                }
            }
        } catch (IOException e) {
            System.out.println("ERROR searching news by id: " + e.getMessage());
            return null;
        }
        return null;
    }

    public NewsInfo searchNewsInfoOr(String title, String link) throws IOException {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.should(QueryBuilders.matchQuery("header", title));
        queryBuilder.should(QueryBuilders.matchQuery("link", link));
        queryBuilder.minimumShouldMatch(1);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);

        SearchRequest request = new SearchRequest(IndexName);
        request.source(sourceBuilder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            TotalHits total = response.getHits().getTotalHits();
            if (total == null) {
                System.out.println("total is null");
                return null;
            }

            System.out.println("There are " + total.value + " results");

            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                NewsInfo newsInfo = new NewsInfo();
                newsInfo.fromMap(sourceAsMap);
                if (newsInfo != null) {
                    System.out.println("Found news " + newsInfo.getHeader() + ", score " + hit.getScore());
                    return newsInfo;
                } else {
                    System.out.println("ni is null");
                    break;
                }
            }
        } catch (IOException e) {
            System.out.println("ERROR searching news by id: " + e.getMessage());
            return null;
        }
        return null;
    }

    public Map<String, Long> searchNewsInfoDateAggregation() throws IOException {
        DateHistogramAggregationBuilder aggregationBuilder = AggregationBuilders.dateHistogram("news_date_aggregation")
                .field("date")
                .format("dd.MM.yyyy")
                .calendarInterval(DateHistogramInterval.DAY);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregationBuilder);

        SearchRequest request = new SearchRequest(IndexName);
        request.source(sourceBuilder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            Histogram newsDateAggregation = response.getAggregations().get("news_date_aggregation");
            Map<String, Long> result = new HashMap<>();
            for (Histogram.Bucket bucket : newsDateAggregation.getBuckets()) {
                result.put(bucket.getKeyAsString(), bucket.getDocCount());
            }

            return result;
        } catch (IOException e) {
            System.out.println("ERROR searching news by date aggregation: " + e.getMessage());
            return null;
        }
    }

//    public long countNewsByDay(String date) throws IOException {
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        sourceBuilder.query(QueryBuilders.termQuery("date", date));
//        sourceBuilder.aggregation(AggregationBuilders.count("news_count").field("news_id"));
//
//        SearchRequest request = new SearchRequest(IndexName);
//        request.source(sourceBuilder);
//
//        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
//        ParsedAdjacencyMatrix.ParsedBucket aggregation = response.getAggregations().get("news_count");
//        return aggregation.getDocCount();
//    }
//public Map<String, Long> aggregateByDate() throws IOException {
//    SearchResponse sr = client.prepareSearch()
//            .addAggregation(
//                    AggregationBuilders.terms("by_country").field("country")
//                            .subAggregation(AggregationBuilders.dateHistogram("by_year")
//                                    .field("dateOfBirth")
//                                    .calendarInterval(DateHistogramInterval.YEAR)
//                                    .subAggregation(AggregationBuilders.avg("avg_children").field("children"))
//                            )
//            )
//            .execute().actionGet();
//}


}
