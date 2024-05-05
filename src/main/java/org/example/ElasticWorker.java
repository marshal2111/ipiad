package org.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ElasticWorker {
    private static ElasticsearchClient elasticClient;

    ElasticWorker() {
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200)).build();

        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        elasticClient = new ElasticsearchClient(transport);
    }
    public Boolean storeNewsInfo(NewsInfo newsInfo) throws IOException {
        try {
            IndexResponse response = elasticClient.index(i -> i
                    .index("news")
                    .id(newsInfo.getHash())
                    .document(newsInfo)
            );

            System.out.println("Indexed with id " + response.id());
        } catch (IOException e) {
            System.out.println("ERROR storing news info: " + e.getMessage());
            return Boolean.FALSE;
        }

        return Boolean.TRUE;
    }

    public NewsInfo searchNewsInfo(String hash) throws IOException {
        try {
            SearchResponse<NewsInfo> response = elasticClient.search(s -> s
                            .index("news")
                            .query(q -> q
                                    .ids(i -> i
                                            .values(hash))
                            ),
                    NewsInfo.class
            );
            TotalHits total = response.hits().total();
            if (total == null) {
                System.out.println("total is null");
                return null;
            }
            boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

            if (isExactResult) {
                System.out.println("There are " + total.value() + " results");
            } else {
                System.out.println("There are more than " + total.value() + " results");
            }

            List<Hit<NewsInfo>> hits = response.hits().hits();
            for (Hit<NewsInfo> hit : hits) {
                NewsInfo ni = hit.source();
                if (ni != null) {
                    System.out.println("Found news " + ni.getHeader() + ", score " + hit.score());
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

    public NewsInfo searchNewsInfoAnd(String title, String link) throws IOException {
        try {
            Query byTitle = MatchQuery.of(m -> m
                    .field("header")
                    .query(title)
            )._toQuery();
            Query byLink = MatchQuery.of(m -> m
                    .field("link")
                    .query(link)
            )._toQuery();
            SearchResponse<NewsInfo> response = elasticClient.search(s -> s
                            .index("news")
                            .query(q -> q
                                    .bool(b -> b
                                            .must(byTitle)
                                            .must(byLink)
                                    )
                            ),
                    NewsInfo.class
            );

            TotalHits total = response.hits().total();
            if (total == null) {
                System.out.println("total is null");
                return null;
            }
            boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

            if (isExactResult) {
                System.out.println("There are " + total.value() + " results");
            } else {
                System.out.println("There are more than " + total.value() + " results");
            }

            List<Hit<NewsInfo>> hits = response.hits().hits();
            for (Hit<NewsInfo> hit : hits) {
                NewsInfo ni = hit.source();
                if (ni != null) {
                    System.out.println("Found news " + ni.getHeader() + ", score " + hit.score());
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
        try {
            Query byTitle = MatchQuery.of(m -> m
                    .field("header")
                    .query(title)
            )._toQuery();
            Query byLink = MatchQuery.of(m -> m
                    .field("link")
                    .query(link)
            )._toQuery();
            SearchResponse<NewsInfo> response = elasticClient.search(s -> s
                            .index("news")
                            .query(q -> q
                                    .bool(b -> b
                                            .should(byTitle)
                                            .should(byLink)
                                            .minimumShouldMatch("1")
                                    )
                            ),
                    NewsInfo.class
            );

            TotalHits total = response.hits().total();
            if (total == null) {
                System.out.println("total is null");
                return null;
            }
            boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

            if (isExactResult) {
                System.out.println("There are " + total.value() + " results");
            } else {
                System.out.println("There are more than " + total.value() + " results");
            }

            List<Hit<NewsInfo>> hits = response.hits().hits();
            for (Hit<NewsInfo> hit : hits) {
                NewsInfo ni = hit.source();
                if (ni != null) {
                    System.out.println("Found news " + ni.getHeader() + ", score " + hit.score());
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


    public NewsInfo searchAgregations(String title, String link) throws IOException {
        DateRangeAggregationBuilder aggregation =
                AggregationBuilders
                        .dateRange("date-filter")
                        .field("date")
                        .format("dd.mm.yyyy")
                        .addUnboundedTo("2022")
                        .addRange("2022", "2024")
                        .addUnboundedFrom("2024");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("news");

        searchRequest.source(sourceBuilder);
        sourceBuilder.profile(true);

        SearchResponse searchResponse = elasticClient.search(searchRequest, NewsInfo.class);

        Range dateRange = searchResponse.aggregations().get("agg");
        for (Range bucket : dateRange.getBuckets()) {
            String key = bucket.getKeyAsString();
            long docCount = bucket.getDocCount();
            System.out.println("Year: " + key + ", Document Count: " + docCount);
        }
        return null;
    }

}



