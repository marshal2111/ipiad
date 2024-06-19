package org.example;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.XContentType;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ElasticSearchClient {
    static {
        System.setProperty("log4j.configurationFile", "/home/farewelly/crawler/planner/src/main/java/org/task/log4j2.xml");
    }

    private static Logger LOGGER = LogManager.getLogger();


    RestHighLevelClient client;
    String INDEX_NAME = "meows";

    ElasticSearchClient() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        createIndex(INDEX_NAME);
    }

    public void close() throws IOException {
        client.close();
    }

    //  Создание индекса
    private void createIndex(String indexName) {
        try {
            // Проверить, существует ли индекс
            boolean indexExists = client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
            if (indexExists) {
                LOGGER.debug("Index already exists: " + indexName);
                return;
            }

            // Создать индекс
            CreateIndexRequest request = new CreateIndexRequest(indexName);

            request.mapping("{\n" +
                    "      \"properties\": {\n" +
                    "        \"date\": {\n" +
                    "          \"type\": \"date\",\n" +
                    "          \"format\": \"dd.MM.yyyy'T'HH:mm:ss\"\n" +
                    "        }\n" +
                    "  }\n" +
                    "}", XContentType.JSON);
            client.indices().create(request, RequestOptions.DEFAULT);
            LOGGER.info("Создан индекс: " + indexName);
        } catch (IOException e) {
            LOGGER.error("Ошибка создания индекса: " + e.getMessage());
            return;
        }
    }


    //    STORE NI Сохранение информации о новости
    public boolean StoreNews(NewsFull news) throws IOException {
        IndexRequest request = new IndexRequest(INDEX_NAME);
        request.id(news.getHash());

        // Установить формат даты
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy'T'HH:mm:ss");
        String formattedDate = dateFormat.format(news.getDate());

        // Создать карту для хранения источника документа
        Map<String, Object> source = new HashMap<>();
        source.putAll(news.toMap());
        source.put("date", formattedDate);
        LOGGER.debug(source);
        // Установить источник документа
        request.source(source, XContentType.JSON);

        try {
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            // LOGGER.info("Записано с идентификатором: " + response.getId());
            return true;
        } catch (IOException e) {
            LOGGER.error("Ошибка записи новости: " + e.getMessage());
            return false;
        }
    }


    //    SEARCH BY HASH поиск статьи по хэшу
    public NewsFull SearchNews(String hash) throws IOException {
        GetRequest request = new GetRequest(INDEX_NAME, hash);

        try {
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            if (response.isExists()) {
                Map<String, Object> sourceAsMap = response.getSourceAsMap();
                NewsFull newsFull = new NewsFull();
                newsFull.fromMap(sourceAsMap);
                LOGGER.info("Новость найдена: " + newsFull.getHeader());
                return newsFull;
            } else {
                LOGGER.info("Новость не найдена");
                return null;
            }
        } catch (IOException e) {
            LOGGER.error("Ошибка поиска по хэшу новости: " + e.getMessage());
            return null;
        }
    }

    //    Поиск статьи по заголовку И ссылке
    public NewsFull SearchNewsByHeaderAndLink(String title, String link) throws IOException {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(QueryBuilders.matchQuery("header", title));
        queryBuilder.must(QueryBuilders.matchQuery("link", link));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);

        SearchRequest request = new SearchRequest(INDEX_NAME);
        request.source(sourceBuilder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            TotalHits total = response.getHits().getTotalHits();

            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                NewsFull newsFull = new NewsFull();
                newsFull.fromMap(sourceAsMap);
                if (newsFull != null) {
                    LOGGER.debug("Найдена новость " + newsFull.getHeader());
                    return newsFull;
                } else {
                    LOGGER.debug("News == null");
                    break;
                }
            }
        } catch (IOException e) {
            LOGGER.error("Ошибка поиска новости: " + e.getMessage());
            return null;
        }
        return null;
    }

    //  Поиск статьи по заголовку ИЛИ ссылке
    public NewsFull SearchNewsByHeaderOrLink(String title, String link) throws IOException {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.should(QueryBuilders.matchQuery("header", title));
        queryBuilder.should(QueryBuilders.matchQuery("link", link));
        queryBuilder.minimumShouldMatch(1);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);

        SearchRequest request = new SearchRequest(INDEX_NAME);
        request.source(sourceBuilder);

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            TotalHits total = response.getHits().getTotalHits();
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                NewsFull newsFull = new NewsFull();
                newsFull.fromMap(sourceAsMap);
                if (newsFull != null) {
                    LOGGER.debug("Найдена новость " + newsFull.getHeader());
                    return newsFull;
                } else {
                    LOGGER.error("ni == null");
                    break;
                }
            }
        } catch (IOException e) {
            LOGGER.error("Ошибка поиска новости: " + e.getMessage());
            return null;
        }
        return null;
    }

    //   Aggregation by date
    public Map<String, Long> AggregateNewsByDate() throws IOException {
        DateHistogramAggregationBuilder aggregationBuilder = AggregationBuilders.dateHistogram("news_date_aggregation")
                .field("date")
                .format("dd.MM.yyyy'T'HH:mm:ss")
                .calendarInterval(DateHistogramInterval.DAY);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(aggregationBuilder);

        SearchRequest request = new SearchRequest(INDEX_NAME);
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
            LOGGER.error("Ошибка поиска новостей: " + e.getMessage());
            return null;
        }
    }

    // MULTIGET 
    public List<NewsFull> MultiGetNewsInfo(List<String> hashes) throws IOException {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (String hash : hashes) {
            multiGetRequest.add(new MultiGetRequest.Item(INDEX_NAME, hash));
        }

        try {
            MultiGetResponse multiGetResponse = client.mget(multiGetRequest, RequestOptions.DEFAULT);
            List<NewsFull> newsFulls = new ArrayList<>();
            for (MultiGetItemResponse itemResponse : multiGetResponse.getResponses()) {
                GetResponse getResponse = itemResponse.getResponse();
                if (getResponse.isExists()) {
                    Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                    NewsFull newsFull = new NewsFull();
                    newsFull.fromMap(sourceAsMap);
                    newsFulls.add(newsFull);
                }
            }
            return newsFulls;
        } catch (IOException e) {
            LOGGER.error("Ошибка MultiGet: " + e.getMessage());
            return null;
        }
    }

    // Аггрегация в диапазоне дат
    public Map<String, Long> SearchNewsNewsByDatetimeRange(String startDate, String endDate) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HashMap<String, Long> res = new HashMap<>();
        sourceBuilder.query(QueryBuilders.rangeQuery("date")
                .gte(startDate)
                .lte(endDate));

        sourceBuilder.aggregation(AggregationBuilders.dateHistogram("date_histogram")
                .field("date")
                .calendarInterval(DateHistogramInterval.DAY)
                .format("dd.MM.yyyy'T'HH:mm:ss"));

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            Histogram dateHistogram = searchResponse.getAggregations().get("date_histogram");
            for (Histogram.Bucket bucket : dateHistogram.getBuckets()) {
                String date = bucket.getKeyAsString();
                long count = bucket.getDocCount();
                res.put(date, count);
            }
        } catch (IOException e) {
            LOGGER.error("ERROR searching documents: " + e.getMessage());
            return null;
        }
        return res;
    }


    // Полнотекстовый поиск в тексте статьи.
    public List<NewsFull> SearchNewsByText(String query) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        List<NewsFull> newsFulls = new ArrayList<>();
        sourceBuilder.query(QueryBuilders.multiMatchQuery(query, "text"));

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                NewsFull newsFull = new NewsFull();
                newsFull.fromMap(sourceAsMap);
                newsFulls.add(newsFull);
            }
        } catch (IOException e) {
            LOGGER.error("Ошибка поиска: " + e.getMessage());
            return null;
        }
        return newsFulls;
    }

    public List<NewsFull> SearchNewsByHeaderAndText(String header, String text, String date) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        List<NewsFull> res = new ArrayList<>();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("header", header));
        boolQueryBuilder.should(QueryBuilders.matchQuery("text", text));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("date").gte(date));

        sourceBuilder.query(boolQueryBuilder);

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                NewsFull newsFull = new NewsFull();
                newsFull.fromMap(sourceAsMap);
                res.add(newsFull);
            }
        } catch (IOException e) {
            LOGGER.error("Ошибка поиска: " + e.getMessage());
            return null;
        }
        return res;
    }


    // METRICS AGGR Подсчет новостей за конкретную дату
    public long CountNewsByDate(String date) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // устанавливаем фильтр по дате
        sourceBuilder.query(QueryBuilders.rangeQuery("date")
                .gte(date+"T00:00:00")
                .lte(date+"T23:59:00"));

        // добавляем metrics агрегацию по количеству новостей
        sourceBuilder.aggregation(AggregationBuilders.cardinality("news_count").field("hash.keyword"));

        // устанавливаем размер результата в 0, чтобы не возвращались сами документы
        sourceBuilder.size(0);

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            Cardinality newsCount = searchResponse.getAggregations().get("news_count");
            long count = newsCount.getValue();
            return count;
        } catch (IOException e) {
            LOGGER.error("Ошибка поиска: " + e.getMessage());
            return 0;
        }
    }

    //   LOGSTASH AGGR Подсчет логов по типу (INFO, ERROR)
    public long CountLogsByLevel(String level) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // устанавливаем фильтр по уровню лога с использованием multi-match query
        sourceBuilder.aggregation(AggregationBuilders.filter("info_count",
                QueryBuilders.multiMatchQuery(level, "message")));

        // устанавливаем размер результата в 0, чтобы не возвращались сами документы
        sourceBuilder.size(0);

        SearchRequest searchRequest = new SearchRequest("logs");
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            Filter infoCount = searchResponse.getAggregations().get("info_count");
            long count = infoCount.getDocCount();
//            log.debug("Level: " + level + ", количество логов: " + count);
            return count;
        } catch (IOException e) {
            LOGGER.error("Ошибка поиска логов: " + e.getMessage());
            return 0;
        }
    }

    public List<String> searchNewsWithLink(int size) throws IOException {
        SearchRequest request = new SearchRequest("meows");
        request.source().size(size);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.existsQuery("link"));
        boolQueryBuilder.filter(QueryBuilders.scriptQuery(
                new Script("doc['link.keyword'].value instanceof String")));

        request.source().query(boolQueryBuilder);
        request.source().fetchSource(new String[]{"header", "link"}, null);

        try {
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            SearchHit[] hits = searchResponse.getHits().getHits();
            List<String> results = new ArrayList<>();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                if (sourceAsMap.containsKey("header")) {
                    String header = (String) sourceAsMap.get("header");
                    results.add(header);
                }
            }
            return results;
        } catch (IOException e) {
            LOGGER.error("Ошибка поиска: " + e.getMessage());
            return null;
        }
    }
}
