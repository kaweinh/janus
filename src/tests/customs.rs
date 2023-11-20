use axum::Extension;
use axum::http::StatusCode;
use axum_test_helper::TestClient;
use axum::async_trait;
use tower_http::cors::{Any, CorsLayer};
use serde::{Serialize, Deserialize};
use sqlx::FromRow;
use crate::{EndpointVerb, ObjectPermission, AccessPermission, InputSerializer, CrudConfig, SchemaTrait, FieldValue, KeyValue};
use axum::Router;
use axum::Json;
use sqlx::postgres::{PgPool, PgRow};

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
struct TestObject {
    id: uuid::Uuid,
    name: String,
    age: i32,
    date_created: chrono::DateTime<chrono::Utc>,
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
    id: Option<uuid::Uuid>,
    name: Option<String>,
    age: Option<i32>,
    age_gt: Option<i32>,
    age_lt: Option<i32>,
    age_ge: Option<i32>,
    age_le: Option<i32>,
    status: Option<String>,
    date_created_gt: Option<chrono::DateTime<chrono::Utc>>,
    date_created_lt: Option<chrono::DateTime<chrono::Utc>>,
    order_by: Option<String>,
    order_dir: Option<String>
}

impl KeyValue for TestObject {
    fn key_value_pairs<'a>(&'a self) -> Vec<(&'static str, FieldValue<'a>)> {
        vec![
            ("id", FieldValue::UUID( &self.id )),
            ("name", FieldValue::STRING( &self.name )),
            ("age", FieldValue::INTEGER( &self.age )),
            ("date_created", FieldValue::DATE( &self.date_created )),
            ("status", FieldValue::STRING( &self.status )),
            ("user_id", FieldValue::STRING( &self.user_id ))
        ]
    }
}

impl KeyValue for TestObjectInputParams {
    fn key_value_pairs<'a>(&'a self) -> Vec<(&'static str, FieldValue<'a>)> {
        vec![
            ("name", FieldValue::STRING( &self.name )),
            ("age", FieldValue::INTEGER( &self.age ))
        ]
    }
}

impl KeyValue for TestObjectQueryParams {
    fn key_value_pairs<'a>(&'a self) -> Vec<(&'static str, FieldValue<'a>)> {
        let mut pairs = vec![];

        if let Some(id) = &self.id {
            pairs.push( ("id", FieldValue::UUID( id )) );
        }

        if let Some(name) = &self.name {
            pairs.push( ("name", FieldValue::STRING( name )) );
        }

        if let Some(age) = &self.age {
            pairs.push( ("age", FieldValue::INTEGER( age )) );
        }

        if let Some(age_gt) = &self.age_gt {
            pairs.push( ("age_gt", FieldValue::INTEGER( age_gt )) );
        }

        if let Some(age_lt) = &self.age_lt {
            pairs.push( ("age_lt", FieldValue::INTEGER( age_lt )) );
        }

        if let Some(age_ge) = &self.age_ge {
            pairs.push( ("age_ge", FieldValue::INTEGER( age_ge )) );
        }

        if let Some(age_le) = &self.age_le {
            pairs.push( ("age_le", FieldValue::INTEGER( age_le )) );
        }

        if let Some(status) = &self.status {
            pairs.push( ("status", FieldValue::STRING( status )) );
        }

        if let Some(date_created_gt) = &self.date_created_gt {
            pairs.push( ("date_created_gt", FieldValue::DATE( date_created_gt )) );
        }

        if let Some(date_created_lt) = &self.date_created_lt {
            pairs.push( ("date_created_lt", FieldValue::DATE( date_created_lt )) );
        }

        if let Some(order_by) = &self.order_by {
            pairs.push( ("order_by", FieldValue::STRING( order_by )) );
        }

        if let Some(order_dir) = &self.order_dir {
            pairs.push( ("order_dir", FieldValue::STRING( order_dir )) );
        }

        return pairs;
    }
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
            id: uuid::Uuid::new_v4(), 
            name: self.name.clone(), 
            age: self.age, 
            date_created: chrono::Utc::now(), 
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
                id UUID PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INT NOT NULL,
                date_created TIMESTAMPTZ NOT NULL,
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
    async fn custom_create<T: KeyValue>( connection_pool: &PgPool, values: T ) -> Result<Json<uuid::Uuid>, StatusCode> where T: Send + Unpin {
        return Ok( Json( uuid::Uuid::parse_str( "96fcebe4-fab9-484e-a28d-cbb1a6216b72" ).unwrap() ) );
    }

    #[allow(unused_variables)]
    async fn custom_read<T, QP>( connection_pool: &PgPool, filters: QP, user_id: Option<String> ) -> Result<Json<Vec<T>>, StatusCode> where T: for<'r> FromRow<'r, PgRow> + Send + Unpin, QP: Send + Sync + Unpin + KeyValue {
        return Ok( Json( vec![] ) );
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaConfig {}
impl SchemaTrait for SchemaConfig {
    fn schema() -> String {
        return "
            CREATE TABLE IF NOT EXISTS TestObjects (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INT NOT NULL,
                date_created VARCHAR(50) NOT NULL,
                status VARCHAR(50) NOT NULL,
                user_id VARCHAR(50) NOT NULL
            );
        ".to_string();
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
    let id = response.json::<uuid::Uuid>().await;
    assert_eq!(id, uuid::Uuid::parse_str( "96fcebe4-fab9-484e-a28d-cbb1a6216b72" ).unwrap() );

}

