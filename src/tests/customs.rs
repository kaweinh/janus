use axum::Extension;
use axum_test_helper::TestClient;
use axum::async_trait;
use tower_http::cors::{Any, CorsLayer};
use serde::{Serialize, Deserialize};
use sqlx::FromRow;
use crate::{EndpointVerb, ObjectPermission, AccessPermission, InputSerializer, CrudConfig, SchemaTrait};
use axum::Router;
use time::OffsetDateTime;
use std::collections::HashMap;
use axum::Json;
use serde_json::Value;
use http::StatusCode;
use sqlx::postgres::{PgPool, PgRow};

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
        return TestObject { 
            id: 0, 
            name: self.name.clone(), 
            age: self.age, 
            date_created: OffsetDateTime::now_utc().format( &time::format_description::well_known::Iso8601::DEFAULT ).unwrap().as_str()[0..19].to_string(), 
            status: "active".to_string(), 
            user_id: match user_id { Some(id) => id, None => "nobody".to_string() } 
        };
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
            EndpointVerb::PUT => false,
            EndpointVerb::DELETE => false
        }
    }

    fn is_custom( verb: &EndpointVerb ) -> bool {
        match verb {
            EndpointVerb::GET => true,
            EndpointVerb::POST => true,
            _ => false
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

    #[allow(unused_variables)]
    async fn custom_create( connection_pool: &PgPool, table_name: &str, values: &HashMap<String, Value> ) -> Result<Json<i32>, StatusCode> {
        return Ok( Json( 0 ) );
    }

    #[allow(unused_variables)]
    async fn custom_read<T>( connection_pool: &PgPool, table_name: &str, filters: &HashMap<String, Value>, user_id: Option<String> ) -> Result<Json<Vec<T>>, StatusCode> where T: for<'r> FromRow<'r, PgRow> + Send + Unpin {
        return Ok( Json( vec![] ) );
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaConfig {}
impl SchemaTrait for SchemaConfig {
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
        .nest("/tableCommands", crate::create_tables_router::<SchemaConfig>())
        .layer(Extension(connection_pool))
        .layer(CorsLayer::new().allow_origin(Any))
}

#[tokio::test]
async fn test_customs() {
    let client = TestClient::new( app_test_setup().await );

    let response = client.post("/tableCommands/dropTable").json(&TestObject::table_name()).send().await;
    assert_eq!(response.status(), 200);
    let response = client.post("/tableCommands/initTables").send().await;
    assert_eq!(response.status(), 200);


    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John Doe".to_string(), age: 30 }).send().await;
    assert_eq!(response.status(), 200);
    let id = response.json::<i32>().await;
    assert_eq!(id, 0);

}

