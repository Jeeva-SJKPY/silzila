package com.silzila.api;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import org.hamcrest.Matchers;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;

public class ApiTest {

    static HashMap<String, String> dbNameToIdMap = new HashMap<>();

    @Test
    public void testGetAllDBConnection() {
        RestAssured.authentication = RestAssured.basic("sjeeva.mail@gmail.com", "Silzila@123");

        ValidatableResponse validatableResponse = RestAssured.given().when().get("http://localhost:8080/api/database-connection").then();
        Response response = validatableResponse.extract().response();

        validatableResponse.log().all();
        validatableResponse.assertThat().statusCode(200);
        JSONArray responseObject = new JSONArray(response.body().asString());
        for(int i=0; i<responseObject.length(); i++){
            JSONObject dbConnectionInfo = responseObject.getJSONObject(i);
            Assert.assertTrue(dbConnectionInfo.has("id"));
            Assert.assertTrue(dbConnectionInfo.has("userId"));
            Assert.assertTrue(dbConnectionInfo.has("vendor"));
            Assert.assertTrue(dbConnectionInfo.has("server"));
            Assert.assertTrue(dbConnectionInfo.has("port"));
            Assert.assertTrue(dbConnectionInfo.has("database"));
            Assert.assertTrue(dbConnectionInfo.has("username"));
            Assert.assertTrue(dbConnectionInfo.has("connectionName"));
            Assert.assertTrue(dbConnectionInfo.has("httpPath"));
            Assert.assertTrue(dbConnectionInfo.has("projectId"));
            Assert.assertTrue(dbConnectionInfo.has("clientEmail"));
            Assert.assertTrue(dbConnectionInfo.has("fileName"));
            Assert.assertTrue(dbConnectionInfo.has("keystoreFileName"));
            Assert.assertTrue(dbConnectionInfo.has("truststoreFileName"));
            Assert.assertTrue(dbConnectionInfo.has("warehouse"));
            dbNameToIdMap.put(dbConnectionInfo.get("vendor").toString(), dbConnectionInfo.get("id").toString());
        }

    }

    private void testGetOneDBConnection(String dbId){
        String url = "http://localhost:8080/api/database-connection/" + dbId;
        ValidatableResponse validatableResponseForConnection = RestAssured.given().when().get(url).then();
        Response responseOfConnection = validatableResponseForConnection.extract().response();

        validatableResponseForConnection.log().all();
        validatableResponseForConnection.assertThat().statusCode(200);
        JSONObject resultObject = new JSONObject(responseOfConnection.body().asString());
        Assert.assertTrue(resultObject.has("id"));
        Assert.assertEquals(resultObject.get("id").toString(), dbId);

    }
    @Test
    public void testAndCreateDBConnection(){

        String testUrl="http://localhost:8080/api/database-connection-test";
        String createUrl="http://localhost:8080/api/database-connection";

        String dbConnectionInfo="{\"connectionName\": \"Pg1\",\"vendor\": \"postgresql\",\"server\": \"localhost\",\"port\": \"5432\",\"database\": \"postgres\",\"password\": \"postgres\",\"username\": \"postgres\"}";

        ValidatableResponse testDBConnection = RestAssured.given().contentType("application/json").body(dbConnectionInfo).when().post(testUrl).then();
        testDBConnection.statusCode(200);

        ValidatableResponse createDBConnection = RestAssured.given().contentType("application/json").body(dbConnectionInfo).when().post(createUrl).then();
        Response createDBResponse = createDBConnection.statusCode(200).extract().response();
        JSONObject responseJson = new JSONObject(createDBResponse.body().asString());

        String dbId = responseJson.getString("id");
        testGetOneDBConnection(dbId);
    }

}

