use axum::Extension;
use axum_test_helper::TestClient;
use axum::async_trait;
use tower_http::cors::{Any, CorsLayer};
use serde::{Serialize, Deserialize};
use sqlx::FromRow;
use crate::{EndpointVerb, ObjectPermission, AccessPermission, InputSerializer, CrudConfig};
use axum::Router;
use time::OffsetDateTime;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
struct TestObject {
    id: i32,
    name: String,
    age: i32,
    date_created: String,
    status: String,
    user_id: String
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestObjectInputParams {
    name: String,
    age: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestObjectQueryParams {
    id: Option<i32>,
    name: Option<String>,
    age: Option<i32>,
    age_gt: Option<i32>,
    age_lt: Option<i32>,
    age_ge: Option<i32>,
    age_le: Option<i32>,
    status: Option<String>,
    date_created_gt: Option<String>,
    date_created_lt: Option<String>,
    order_by: Option<String>,
    order_dir: Option<String>
}

impl InputSerializer<TestObject> for TestObjectInputParams {
    fn verify(&self) -> bool {
        if self.age > 0 && self.age < 100 && self.name.len() > 0 && self.name.len() < 50 {
            return true;
        } else {
            return false;
        }
    }

    fn add_set_values(&self, user_id: Option<String>) -> TestObject {
        return TestObject { id: 0, name: self.name.clone(), age: self.age, date_created: OffsetDateTime::now_utc().format( &time::format_description::well_known::Iso8601::DEFAULT ).unwrap().as_str()[0..19].to_string(), status: "active".to_string(), user_id: match user_id { Some(id) => id, None => "nobody".to_string() } };
    }
}

#[async_trait]
impl CrudConfig for TestObject {
    fn table_name() -> &'static str {
        return "TestObjects";
    }

    fn endpoint_name() -> &'static str {
        return "testObjects";
    }

    fn schema() -> &'static str {
        return "
            CREATE TABLE IF NOT EXISTS TestObjects (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INT NOT NULL,
                date_created VARCHAR(50) NOT NULL,
                status VARCHAR(50) NOT NULL,
                user_id VARCHAR(50) NOT NULL
            );
        ";
    }

    fn include_endpoint( verb: &EndpointVerb ) -> bool {
        match verb {
            EndpointVerb::GET => true,
            EndpointVerb::POST => true,
            EndpointVerb::PUT => true,
            EndpointVerb::DELETE => true
        }
    }

    fn is_custom( verb: &EndpointVerb ) -> bool {
        match verb {
            EndpointVerb::GET => false,
            EndpointVerb::POST => false,
            EndpointVerb::PUT => false,
            EndpointVerb::DELETE => false
        }
    }

    fn get_object_permissions( verb: &EndpointVerb ) -> ObjectPermission {
        match verb {
            EndpointVerb::GET => ObjectPermission::ALL,
            EndpointVerb::POST => ObjectPermission::ALL,
            EndpointVerb::PUT => ObjectPermission::ALL,
            EndpointVerb::DELETE => ObjectPermission::ALL
        }
    }

    fn get_access_permissions( verb: &EndpointVerb ) -> AccessPermission {
        match verb {
            EndpointVerb::GET => AccessPermission::ANY,
            EndpointVerb::POST => AccessPermission::ANY,
            EndpointVerb::PUT => AccessPermission::ANY,
            EndpointVerb::DELETE => AccessPermission::ANY
        }
    }
}

pub async fn app_test_setup() -> Router {
    dotenv::dotenv().ok();
    
    let resp = crate::connect_to_db(
                                                    std::env::var("DB_HOST").unwrap().as_str(), 
                                                    5432,
                                                    std::env::var("DB_USERNAME").unwrap().as_str(), 
                                                    std::env::var("DB_PASSWORD").unwrap().as_str(), 
                                                    std::env::var("DB_NAME").unwrap().as_str() 
                                                )
                                                .await;

    let connection_pool = match resp {
        Ok(pool) => pool,
        Err(e) => panic!("Failed to connect to database: {:?}", e)
    };
    
    Router::new()
        .nest("/restful", crate::create_endpoint_router::<TestObject, TestObjectInputParams, TestObjectQueryParams>())
        .nest("/tableCommands", crate::create_tables_router())
        .layer(Extension(connection_pool))
        .layer(CorsLayer::new().allow_origin(Any))
}

#[tokio::test]
async fn test_post_any() {
    let client = TestClient::new( app_test_setup().await );

    let response = client.post("/tableCommands/dropTable").json(&TestObject::table_name()).send().await;
    assert_eq!(response.status(), 200);
    let response = client.post("/tableCommands/initTables").json(&TestObject::schema()).send().await;
    assert_eq!(response.status(), 200);

    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John Doe".to_string(), age: 30 }).send().await;
    assert_eq!(response.status(), 200);

    let map: HashMap<&str, i32> = [("key1", 42), ("key2", 73)].iter().cloned().collect();
    let response = client.post("/restful/testObjects").json( &map ).send().await;
    assert_eq!(response.status(), 422);

    let map2: HashMap<&str, &str> = [("name", "butt"), ("age", "cant")].iter().cloned().collect();
    let response = client.post("/restful/testObjects").json( &map2 ).send().await;
    assert_eq!(response.status(), 422);

    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John Doe".to_string(), age: 101 }).send().await;
    assert_eq!(response.status(), 400);

    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "Hello john doe how is your day going boss man I need to talk to you about somethign going on the world. You ugly bitch.".to_string(), age: 99 }).send().await;
    assert_eq!(response.status(), 400);

    let response = client.get("/restful/testObjects?name=John%20Doe").send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    println!("{:?}", objects);
    assert_eq!( objects[0].user_id, "nobody");

}

#[tokio::test]
async fn test_get_any() {
    let client = TestClient::new( app_test_setup().await );

    let response = client.post("/tableCommands/dropTable").json(&TestObject::table_name()).send().await;
    assert_eq!(response.status(), 200);

    let response = client.post("/tableCommands/initTables").json(&TestObject::schema()).send().await;
    assert_eq!(response.status(), 200);

    let response = client.get("/restful/testObjects?age=hello&myage=world").send().await;
    assert_eq!(response.status(), 400);

    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John".to_string(), age: 30 }).send().await;
    assert_eq!(response.status(), 200);

    let response = client.get("/restful/testObjects?name=John").send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    assert_eq!( objects[0].age, 30);

    let response = client.get("/restful/testObjects?age_lt=35&age_gt=29").send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    assert_eq!( objects[0].name, "John");
}

#[tokio::test]
async fn test_put_any() {
    let client = TestClient::new( app_test_setup().await );

    let response = client.post("/tableCommands/dropTable").json(&TestObject::table_name()).send().await;
    assert_eq!(response.status(), 200);
    let response = client.post("/tableCommands/initTables").json(&TestObject::schema()).send().await;
    assert_eq!(response.status(), 200);

    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John".to_string(), age: 30 }).send().await;
    assert_eq!(response.status(), 200);
    
    let id = response.json::<i32>().await;

    let response = client.put(&format!("/restful/testObjects/{}", id.to_string() )).json(&TestObjectInputParams { name: "John".to_string(), age: 29 }).send().await;
    assert_eq!(response.status(), 200);

    let response = client.get(&format!("/restful/testObjects?id={}", id.to_string() )).send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    assert_eq!( objects[0].age, 29);

    let response = client.put(&format!("/restful/testObjects/{}", id.to_string() )).json(&TestObjectInputParams { name: "John".to_string(), age: 101 }).send().await;
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn test_delete_any() {
    let client = TestClient::new( app_test_setup().await );

    let response = client.post("/tableCommands/dropTable").json(&TestObject::table_name()).send().await;
    assert_eq!(response.status(), 200);
    let response = client.post("/tableCommands/initTables").json(&TestObject::schema()).send().await;
    assert_eq!(response.status(), 200);

    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John".to_string(), age: 30 }).send().await;
    assert_eq!(response.status(), 200);
    
    let id = response.json::<i32>().await;
    let response = client.get(&format!("/restful/testObjects?id={}", id.to_string() )).send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    assert_eq!( objects[0].name, "John");

    let response = client.delete(&format!("/restful/testObjects/{}", id.to_string() )).send().await;
    assert_eq!(response.status(), 200);

    let response = client.get(&format!("/restful/testObjects?id={}", id.to_string() )).send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    assert!( objects.is_empty() );
}

#[tokio::test]
async fn test_order_by_any() {
    let client = TestClient::new( app_test_setup().await );

    let response = client.post("/tableCommands/dropTable").json(&TestObject::table_name()).send().await;
    assert_eq!(response.status(), 200);
    let response = client.post("/tableCommands/initTables").json(&TestObject::schema()).send().await;
    assert_eq!(response.status(), 200);

    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John".to_string(), age: 30 }).send().await;
    assert_eq!(response.status(), 200);
    
    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John2".to_string(), age: 32 }).send().await;
    assert_eq!(response.status(), 200);

    let response = client.get("/restful/testObjects?order_by=age").send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    assert_eq!( objects[0].age, 32);

    let response = client.get("/restful/testObjects?order_by=age&order_dir=asc").send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    assert_eq!( objects[0].age, 30);
}

#[tokio::test]
async fn test_datetime() {
    let client = TestClient::new( app_test_setup().await );

    let response = client.post("/tableCommands/dropTable").json(&TestObject::table_name()).send().await;
    assert_eq!(response.status(), 200);
    let response = client.post("/tableCommands/initTables").json(&TestObject::schema()).send().await;
    assert_eq!(response.status(), 200);

    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John".to_string(), age: 30 }).send().await;
    assert_eq!(response.status(), 200);


    let response = client.get( &format!("/restful/testObjects?date_created_gt=2023-11-12T00:00:00&date_created_lt=2023-11-15T00:00:00")).send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    assert!( !objects.is_empty() );

    let response = client.get( &format!("/restful/testObjects?date_created_gt=2023-11-16T00:00:00&date_created_lt=2023-11-18T00:00:00")).send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    assert!( objects.is_empty() );
}