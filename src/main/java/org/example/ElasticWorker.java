//package org.example;
//
//import co.elastic.clients.elasticsearch.ElasticsearchClient;
//import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
//import co.elastic.clients.elasticsearch._types.aggregations.DateHistogramAggregation;
//import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
//import co.elastic.clients.elasticsearch._types.query_dsl.Query;
//import co.elastic.clients.elasticsearch.core.IndexResponse;
//import co.elastic.clients.elasticsearch.core.SearchResponse;
//import co.elastic.clients.elasticsearch.core.search.Hit;
//import co.elastic.clients.elasticsearch.core.search.TotalHits;
//import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
//import co.elastic.clients.json.jackson.JacksonJsonpMapper;
//import co.elastic.clients.transport.ElasticsearchTransport;
//import co.elastic.clients.transport.rest_client.RestClientTransport;
//import org.apache.http.HttpHost;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.ml.job.results.Bucket;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.search.aggregations.AggregationBuilder;
//import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import co.elastic.clients.elasticsearch._types.Time;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
//public class ElasticWorker {
//    private static ElasticsearchClient elasticClient;
//
//    ElasticWorker() {
//        RestClient restClient = RestClient.builder(
//                new HttpHost("localhost", 9200)).build();
//
//        ElasticsearchTransport transport = new RestClientTransport(
//                restClient, new JacksonJsonpMapper());
//
//        elasticClient = new ElasticsearchClient(transport);
//    }
//    public Boolean storeNewsInfo(NewsInfo newsInfo) throws IOException {
//        try {
//            IndexResponse response = elasticClient.index(i -> i
//                    .index(IndexName)
//                    .id(newsInfo.getHash())
//                    .document(newsInfo)
//            );
//
//            System.out.println("Indexed with id " + response.id());
//        } catch (IOException e) {
//            System.out.println("ERROR storing news info: " + e.getMessage());
//            return Boolean.FALSE;
//        }
//
//        return Boolean.TRUE;
//    }
//
//    public NewsInfo searchNewsInfo(String hash) throws IOException {
//        try {
//            SearchResponse<NewsInfo> response = elasticClient.search(s -> s
//                            .index(IndexName)
//                            .query(q -> q
//                                    .ids(i -> i
//                                            .values(hash))
//                            ),
//                    NewsInfo.class
//            );
//            TotalHits total = response.hits().total();
//            if (total == null) {
//                System.out.println("total is null");
//                return null;
//            }
//            boolean isExactResult = total.relation() == TotalHitsRelation.Eq;
//
//            if (isExactResult) {
//                System.out.println("There are " + total.value() + " results");
//            } else {
//                System.out.println("There are more than " + total.value() + " results");
//            }
//
//            List<Hit<NewsInfo>> hits = response.hits().hits();
//            for (Hit<NewsInfo> hit : hits) {
//                NewsInfo ni = hit.source();
//                if (ni != null) {
//                    System.out.println("Found news " + ni.getHeader() + ", score " + hit.score());
//                } else {
//                    System.out.println("ni is null");
//                    break;
//                }
//            }
//        } catch (IOException e) {
//            System.out.println("ERROR searching news by id: " + e.getMessage());
//            return null;
//        }
//        return null;
//    }
//
//    public NewsInfo searchNewsInfoAnd(String title, String link) throws IOException {
//        try {
//            Query byTitle = MatchQuery.of(m -> m
//                    .field("header")
//                    .query(title)
//            )._toQuery();
//            Query byLink = MatchQuery.of(m -> m
//                    .field("link")
//                    .query(link)
//            )._toQuery();
//            SearchResponse<NewsInfo> response = elasticClient.search(s -> s
//                            .index(IndexName)
//                            .query(q -> q
//                                    .bool(b -> b
//                                            .must(byTitle)
//                                            .must(byLink)
//                                    )
//                            ),
//                    NewsInfo.class
//            );
//
//            TotalHits total = response.hits().total();
//            if (total == null) {
//                System.out.println("total is null");
//                return null;
//            }
//            boolean isExactResult = total.relation() == TotalHitsRelation.Eq;
//
//            if (isExactResult) {
//                System.out.println("There are " + total.value() + " results");
//            } else {
//                System.out.println("There are more than " + total.value() + " results");
//            }
//
//            List<Hit<NewsInfo>> hits = response.hits().hits();
//            for (Hit<NewsInfo> hit : hits) {
//                NewsInfo ni = hit.source();
//                if (ni != null) {
//                    System.out.println("Found news " + ni.getHeader() + ", score " + hit.score());
//                } else {
//                    System.out.println("ni is null");
//                    break;
//                }
//            }
//        } catch (IOException e) {
//            System.out.println("ERROR searching news by id: " + e.getMessage());
//            return null;
//        }
//        return null;
//    }
//
//    public NewsInfo searchNewsInfoOr(String title, String link) throws IOException {
//        try {
//            Query byTitle = MatchQuery.of(m -> m
//                    .field("header")
//                    .query(title)
//            )._toQuery();
//            Query byLink = MatchQuery.of(m -> m
//                    .field("link")
//                    .query(link)
//            )._toQuery();
//            SearchResponse<NewsInfo> response = elasticClient.search(s -> s
//                            .index(IndexName)
//                            .query(q -> q
//                                    .bool(b -> b
//                                            .should(byTitle)
//                                            .should(byLink)
//                                            .minimumShouldMatch("1")
//                                    )
//                            ),
//                    NewsInfo.class
//            );
//
//            TotalHits total = response.hits().total();
//            if (total == null) {
//                System.out.println("total is null");
//                return null;
//            }
//            boolean isExactResult = total.relation() == TotalHitsRelation.Eq;
//
//            if (isExactResult) {
//                System.out.println("There are " + total.value() + " results");
//            } else {
//                System.out.println("There are more than " + total.value() + " results");
//            }
//
//            List<Hit<NewsInfo>> hits = response.hits().hits();
//            for (Hit<NewsInfo> hit : hits) {
//                NewsInfo ni = hit.source();
//                if (ni != null) {
//                    System.out.println("Found news " + ni.getHeader() + ", score " + hit.score());
//                } else {
//                    System.out.println("ni is null");
//                    break;
//                }
//            }
//        } catch (IOException e) {
//            System.out.println("ERROR searching news by id: " + e.getMessage());
//            return null;
//        }
//        return null;
//    }
//}
//
//
//
