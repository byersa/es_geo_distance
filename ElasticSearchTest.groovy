import spock.lang.*;

import org.moqui.context.ExecutionContext
import org.moqui.Moqui

import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.client.AdminClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.search.SearchHits
import org.elasticsearch.index.query.QueryStringQueryBuilder
import org.elasticsearch.search.query.QuerySearchRequest
import org.elasticsearch.search.query.QuerySearchResult
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.index.query.GeoDistanceRangeFilterBuilder
import org.elasticsearch.index.query.GeoDistanceFilterBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.unit.DistanceUnit
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse


class ElasticSearchTest extends Specification {

    protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticSearchTest.class)

    @Shared
    ExecutionContext ec

    def setupSpec() {
        logger.info("In: ElasticSearchTest, setupSpec")
        ec = Moqui.getExecutionContext()
        logger.info("In: ElasticSearchTest, ec: ${ec}")
    }

    def cleanupSpec() {
        logger.info("In: ElasticSearchTest, cleanupSpec")
        ec.destroy()
    }

    def setup() {
        logger.info("In: ElasticSearchTest, setup")
        ec.artifactExecution.disableAuthz()
    }

    def cleanup() {
        logger.info("In: ElasticSearchTest, cleanup")
        ec.artifactExecution.enableAuthz()
    }


    def "delete index"() {
	when:
            DeleteIndexResponse deleteIndexResponse = ec.elasticSearchClient.admin().indices().delete(new DeleteIndexRequest('rcherbals')).actionGet()
            logger.info("delete index, :" +  deleteIndexResponse.isAcknowledged())
        then:
            deleteIndexResponse.isAcknowledged()
	}

    def "create index"() {
	when:
            Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "ElasticSearchClient").build()
            logger.info("create index, settings: ${settings}")
            CreateIndexRequestBuilder createIndexRequestBuilder = ec.elasticSearchClient.admin().indices().prepareCreate("rcherbals")
            logger.info("create index, createIndexRequestBuilder: ${createIndexRequestBuilder}")
            String geoMapStr = """{ 
                         "properties": { 
                              "location": { "type": "geo_point", "lat_lon": "true"} 
                               } 
                         }"""

            Map geoMap = new HashMap()
            Map propMap = new HashMap()
            Map locationMap = new HashMap()
            locationMap.put("type", "geo_point")
            locationMap.put("lat_lon", "true")
            propMap.put("location", locationMap);
            geoMap.put("properties", propMap);
        
            logger.info("create index, geoMap: ${geoMap}")
            createIndexRequestBuilder.addMapping("object", geoMap)
            CreateIndexResponse response = createIndexRequestBuilder.execute().actionGet()
            logger.info("get elasticsearch, :" +  response.isAcknowledged())
	then:
            response.isAcknowledged()
        }

    def "load documents"() {
	when:
	    //String doc1 = """{"location" : { "lat" : 41.12, "lon" : -71.34 } }"""
	    Map doc1 = new HashMap()
            Map fieldMap = new HashMap()
            fieldMap.put("lat", 41.12)
            fieldMap.put("lon", -71.34)
            doc1.put("location", fieldMap)
            //"""{"location" : { "lat" : 41.12, "lon" : -71.34 } }"""
            logger.info("load documents, doc1: ${doc1}")
            IndexResponse response = ec.elasticSearchClient
                        .prepareIndex("rcherbals", "object", "100101")
                        .setSource(doc1).execute().actionGet()
            logger.info("load documents 1, response.index: ${response.getIndex()}, response.type: ${response.getType()}, id: ${response.getId()}, version: ${response.getVersion()}")

	    //String doc2 = """{"location" : { "lat" : 31.12, "lon" : -61.34 } }"""
	    Map doc2 = new HashMap()
            Map fieldMap2 = new HashMap()
            fieldMap2.put("lat", 31.12)
            fieldMap2.put("lon", -61.34)
            doc2.put("location", fieldMap2)
            logger.info("load documents, doc2: ${doc2}")
            IndexResponse response2 = ec.elasticSearchClient
                        .prepareIndex("rcherbals", "object", "100102")
                        .setSource(doc2).execute().actionGet()
            logger.info("load documents 2, response: ${response}")

		RefreshResponse refreshResponse = ec.elasticSearchClient.admin().indices().refresh(new RefreshRequest("rcherbals")).actionGet()
                logger.info("load documents, refreshResponse: ${refreshResponse.toString()}")

		SearchRequestBuilder srb = ec.elasticSearchClient.prepareSearch() //.setIndices("rcherbals").setTypes("object")
		srb.setQuery(QueryBuilders.matchAllQuery())
                srb.setFrom(0).setSize(10)
		SearchResponse searchResponse = srb.execute().actionGet()
                logger.info("load documents, searchResponse: ${searchResponse.toString()}")
                SearchHits hits = searchResponse.getHits()
                logger.info("load documents, hits: ${hits}")
                logger.info("load documents, total: ${hits.getTotalHits()}")

	then:
		response.getId()
		response2.getId()
        }

    def "query documents"() {
	when:
               //SearchHits hits = ec.elasticSearchClient.prepareSearch().setIndices(indexName).setTypes(documentType)
                        //.setQuery(QueryBuilders.queryString(queryString)).setFrom(fromOffset).setSize(sizeLimit).execute().actionGet().getHits()
		RefreshResponse refreshResponse = ec.elasticSearchClient.admin().indices().refresh(new RefreshRequest("rcherbals")).actionGet()
                logger.info("load documents, refreshResponse: ${refreshResponse.toString()}")
		SearchRequestBuilder srb = ec.elasticSearchClient.prepareSearch().setIndices("rcherbals").setTypes("object")
                logger.info("query documents, srb: ${srb}")
		srb.setQuery(QueryBuilders.matchAllQuery())
                srb.setFrom(0).setSize(10)
                logger.info("query documents, srb 2: ${srb}")
		GeoDistanceFilterBuilder fb = FilterBuilders.geoDistanceFilter("location")
                logger.info("query documents, fb: ${fb}")
		fb.lat(31.12).lon(-61.34).distance(10, DistanceUnit.KILOMETERS)
		srb.setFilter(fb)
                logger.info("query documents, fb 2: ${fb}")
		SearchResponse response = srb.execute().actionGet()
                logger.info("query documents, response: ${response}")
                SearchHits hits = response.getHits()
                logger.info("query documents, hits: ${hits}")
	then:
                hits.totalHits()
        }
}
