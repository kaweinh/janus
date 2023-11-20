use axum::Extension;
use axum_test_helper::TestClient;
use axum::async_trait;
use tower_http::cors::{Any, CorsLayer};
use serde::{Serialize, Deserialize};
use sqlx::FromRow;
use crate::{EndpointVerb, ObjectPermission, AccessPermission, InputSerializer, CrudConfig, SchemaTrait, FieldValue, KeyValue};
use axum::Router;

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
            EndpointVerb::GET => ObjectPermission::OWNER,
            EndpointVerb::POST => ObjectPermission::OWNER,
            EndpointVerb::PUT => ObjectPermission::OWNER,
            EndpointVerb::DELETE => ObjectPermission::OWNER
        }
    }

    fn get_access_permissions( verb: &EndpointVerb ) -> AccessPermission {
        match verb {
            EndpointVerb::GET => AccessPermission::AUTHENTICATED,
            EndpointVerb::POST => AccessPermission::AUTHENTICATED,
            EndpointVerb::PUT => AccessPermission::AUTHENTICATED,
            EndpointVerb::DELETE => AccessPermission::ADMIN
        }
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
async fn test_post_auth() {
    let token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InAxNGgwTF80bVZmWnUxckstVThvWCJ9.eyJpc3MiOiJodHRwczovL2F1dGguYXJic3Zzb2Rkcy5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDY1ODE3NjMxODcxNjQ0OTI5ODciLCJhdWQiOlsiaHR0cHM6Ly9hcmJzdnNvZGRzLmNvbS9hcGkiLCJodHRwczovL2Rldi1tYXI0bG10eGc4eXh1Y3JwLnVzLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2OTk5MTQzNDcsImV4cCI6MTcwMDAwMDc0NywiYXpwIjoiR3hNbWVJUjF5amVZZHA2clN6bWRES0hPQVE3YTVSNFMiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIiwicGVybWlzc2lvbnMiOlsicmVhZDphZG1pbiJdfQ.CoT9r61MTIbtfmWykzodLzoNtYtLbuDNyw5IZzHpw8vxRkBhCyGXpk6LnslhOwZP6gb6nAShg1AnUtimUNIXVy76LD5o8yo19XQxZ9cYKjfVic8ix9o4DMDoKSl0DAdkfJeBJhuGi31svBC90nOM9JSRm5W8XhawxNxnyi07wdX9BMVklZGqgQpXzrnOGC995YL7rXZwwIiZ4In7uYn9IlopBvseOXvvCVG2Q_Sm8EaI8aXFOGYVnwfKPvJWv45oJ9xabi9SsHY8ONR4pV38Q3dbum-glIy4NW0u-zMuDALUBCAfRMQulLPq5ftWolcFyCA979142TT_waD_XkWdzA";
    let client = TestClient::new( app_test_setup().await );

    let response = client.post("/tableCommands/dropTable").json(&TestObject::table_name()).send().await;
    assert_eq!(response.status(), 200);
    let response = client.post("/tableCommands/initTables").send().await;
    assert_eq!(response.status(), 200);

    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John Doe".to_string(), age: 30 }).send().await;
    assert_eq!(response.status(), 401);

    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John Doe".to_string(), age: 30 }).send().await;
    assert_eq!(response.status(), 401);

    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John Doe".to_string(), age: 30 }).header("Authorization", token).send().await;
    assert_eq!(response.status(), 401);

    let bearer_token = format!("Bearer {}", token);
    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John Doe".to_string(), age: 30 }).header("Authorization", &bearer_token).send().await;
    assert_eq!(response.status(), 200);

    let response = client.get("/restful/testObjects?name=John%20Doe").header("Authorization", &bearer_token).send().await;
    let objects: Vec<TestObject> = response.json().await;
    println!("{:?}", objects);
    assert_eq!( objects[0].user_id, "google-oauth2|106581763187164492987");
}

#[tokio::test]
async fn test_put_auth() {
    let token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InAxNGgwTF80bVZmWnUxckstVThvWCJ9.eyJpc3MiOiJodHRwczovL2F1dGguYXJic3Zzb2Rkcy5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDY1ODE3NjMxODcxNjQ0OTI5ODciLCJhdWQiOlsiaHR0cHM6Ly9hcmJzdnNvZGRzLmNvbS9hcGkiLCJodHRwczovL2Rldi1tYXI0bG10eGc4eXh1Y3JwLnVzLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2OTk5MTQzNDcsImV4cCI6MTcwMDAwMDc0NywiYXpwIjoiR3hNbWVJUjF5amVZZHA2clN6bWRES0hPQVE3YTVSNFMiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIiwicGVybWlzc2lvbnMiOlsicmVhZDphZG1pbiJdfQ.CoT9r61MTIbtfmWykzodLzoNtYtLbuDNyw5IZzHpw8vxRkBhCyGXpk6LnslhOwZP6gb6nAShg1AnUtimUNIXVy76LD5o8yo19XQxZ9cYKjfVic8ix9o4DMDoKSl0DAdkfJeBJhuGi31svBC90nOM9JSRm5W8XhawxNxnyi07wdX9BMVklZGqgQpXzrnOGC995YL7rXZwwIiZ4In7uYn9IlopBvseOXvvCVG2Q_Sm8EaI8aXFOGYVnwfKPvJWv45oJ9xabi9SsHY8ONR4pV38Q3dbum-glIy4NW0u-zMuDALUBCAfRMQulLPq5ftWolcFyCA979142TT_waD_XkWdzA";
    let client = TestClient::new( app_test_setup().await );

    let response = client.post("/tableCommands/dropTable").json(&TestObject::table_name()).send().await;
    assert_eq!(response.status(), 200);
    let response = client.post("/tableCommands/initTables").send().await;
    assert_eq!(response.status(), 200);

    let bearer_token = format!("Bearer {}", token);
    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John Doe".to_string(), age: 30 }).header("Authorization", &bearer_token).send().await;
    assert_eq!(response.status(), 200);
    let id = response.json::<i32>().await;

    let response = client.put(&format!("/restful/testObjects/{}", id.to_string() )).json(&TestObjectInputParams { name: "John".to_string(), age: 29 }).send().await;
    assert_eq!(response.status(), 401);

    let response = client.put(&format!("/restful/testObjects/{}", id.to_string() )).json(&TestObjectInputParams { name: "John".to_string(), age: 29 }).header("Authorization", token).send().await;
    assert_eq!(response.status(), 401);

    let response = client.put(&format!("/restful/testObjects/{}", id.to_string() )).json(&TestObjectInputParams { name: "John".to_string(), age: 29 }).header("Authentication", &bearer_token).send().await;
    assert_eq!(response.status(), 401);

    let response = client.put(&format!("/restful/testObjects/{}", id.to_string() )).json(&TestObjectInputParams { name: "John".to_string(), age: 29 }).header("Authorization", &bearer_token).send().await;
    assert_eq!(response.status(), 200);

    let response = client.get(&format!("/restful/testObjects?id={}", id.to_string() )).header("Authorization", &bearer_token).send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    println!("{:?}", objects);
    assert_eq!( objects[0].user_id, "google-oauth2|106581763187164492987");
    assert_eq!( objects[0].age, 29 );
}

#[tokio::test]
async fn test_delete_admin() {
    let token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InAxNGgwTF80bVZmWnUxckstVThvWCJ9.eyJpc3MiOiJodHRwczovL2F1dGguYXJic3Zzb2Rkcy5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDY1ODE3NjMxODcxNjQ0OTI5ODciLCJhdWQiOlsiaHR0cHM6Ly9hcmJzdnNvZGRzLmNvbS9hcGkiLCJodHRwczovL2Rldi1tYXI0bG10eGc4eXh1Y3JwLnVzLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2OTk5MTQzNDcsImV4cCI6MTcwMDAwMDc0NywiYXpwIjoiR3hNbWVJUjF5amVZZHA2clN6bWRES0hPQVE3YTVSNFMiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIiwicGVybWlzc2lvbnMiOlsicmVhZDphZG1pbiJdfQ.CoT9r61MTIbtfmWykzodLzoNtYtLbuDNyw5IZzHpw8vxRkBhCyGXpk6LnslhOwZP6gb6nAShg1AnUtimUNIXVy76LD5o8yo19XQxZ9cYKjfVic8ix9o4DMDoKSl0DAdkfJeBJhuGi31svBC90nOM9JSRm5W8XhawxNxnyi07wdX9BMVklZGqgQpXzrnOGC995YL7rXZwwIiZ4In7uYn9IlopBvseOXvvCVG2Q_Sm8EaI8aXFOGYVnwfKPvJWv45oJ9xabi9SsHY8ONR4pV38Q3dbum-glIy4NW0u-zMuDALUBCAfRMQulLPq5ftWolcFyCA979142TT_waD_XkWdzA";
    let client = TestClient::new( app_test_setup().await );

    let response = client.post("/tableCommands/dropTable").json(&TestObject::table_name()).send().await;
    assert_eq!(response.status(), 200);
    let response = client.post("/tableCommands/initTables").send().await;
    assert_eq!(response.status(), 200);

    let bearer_token = format!("Bearer {}", token);
    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John Doe".to_string(), age: 30 }).header("Authorization", &bearer_token).send().await;
    assert_eq!(response.status(), 200);
    let id = response.json::<i32>().await;

    let response = client.delete(&format!("/restful/testObjects/{}", id.to_string() )).send().await;
    assert_eq!(response.status(), 401);

    let response = client.delete(&format!("/restful/testObjects/{}", id.to_string() )).header("Authorization", token).send().await;
    assert_eq!(response.status(), 401);

    let response = client.delete(&format!("/restful/testObjects/{}", id.to_string() )).header("Authentication", &bearer_token).send().await;
    assert_eq!(response.status(), 401);

    let response = client.delete(&format!("/restful/testObjects/{}", id.to_string() )).header("Authorization", &bearer_token).send().await;
    assert_eq!(response.status(), 200);

    let response = client.get(&format!("/restful/testObjects?id={}", id.to_string() )).header("Authorization", &bearer_token).send().await;
    assert_eq!(response.status(), 200);

    let objects: Vec<TestObject> = response.json().await;
    assert!( objects.is_empty() );
}

#[tokio::test]
async fn test_user_objects_private() {
    let token_gmail = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InAxNGgwTF80bVZmWnUxckstVThvWCJ9.eyJpc3MiOiJodHRwczovL2F1dGguYXJic3Zzb2Rkcy5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDY1ODE3NjMxODcxNjQ0OTI5ODciLCJhdWQiOlsiaHR0cHM6Ly9hcmJzdnNvZGRzLmNvbS9hcGkiLCJodHRwczovL2Rldi1tYXI0bG10eGc4eXh1Y3JwLnVzLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2OTk5MTQzNDcsImV4cCI6MTcwMDAwMDc0NywiYXpwIjoiR3hNbWVJUjF5amVZZHA2clN6bWRES0hPQVE3YTVSNFMiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIiwicGVybWlzc2lvbnMiOlsicmVhZDphZG1pbiJdfQ.CoT9r61MTIbtfmWykzodLzoNtYtLbuDNyw5IZzHpw8vxRkBhCyGXpk6LnslhOwZP6gb6nAShg1AnUtimUNIXVy76LD5o8yo19XQxZ9cYKjfVic8ix9o4DMDoKSl0DAdkfJeBJhuGi31svBC90nOM9JSRm5W8XhawxNxnyi07wdX9BMVklZGqgQpXzrnOGC995YL7rXZwwIiZ4In7uYn9IlopBvseOXvvCVG2Q_Sm8EaI8aXFOGYVnwfKPvJWv45oJ9xabi9SsHY8ONR4pV38Q3dbum-glIy4NW0u-zMuDALUBCAfRMQulLPq5ftWolcFyCA979142TT_waD_XkWdzA";
    let token_fb = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InAxNGgwTF80bVZmWnUxckstVThvWCJ9.eyJpc3MiOiJodHRwczovL2F1dGguYXJic3Zzb2Rkcy5jb20vIiwic3ViIjoiZmFjZWJvb2t8MzYyNjQwMDk1MDkxMjEwNyIsImF1ZCI6WyJodHRwczovL2FyYnN2c29kZHMuY29tL2FwaSIsImh0dHBzOi8vZGV2LW1hcjRsbXR4Zzh5eHVjcnAudXMuYXV0aDAuY29tL3VzZXJpbmZvIl0sImlhdCI6MTY5OTkxNjU5NSwiZXhwIjoxNzAwMDAyOTk1LCJhenAiOiJHeE1tZUlSMXlqZVlkcDZyU3ptZERLSE9BUTdhNVI0UyIsInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZW1haWwiLCJwZXJtaXNzaW9ucyI6W119.ro6Fa9yt_ewF9PQWoWeKISXzfwHmkjPcgQJ80dofYXST-UVOWXg-l5UivEpsdBV_8NlAjWUiZtz_D5kKZ_QwoDpxMOkUGkhQHr_KvlWg0Cm1G9uTwCFwWLLtqwJ4n-vsjTPl1x983Xhb6iJu79tcEWt56Rppvwhyw0dtPVtQQ0pmLVizumpQo-BatuX8Uy14oDEo7ph5LNsT7MZOkCkq9pDk1LdQU1aNBI_XawPARhhnjLzac5Cz6zY-_UHk8KL_VYLXOp7cuJr3k6AAIIBzg3O9jH1uwo-wBJAkMwgdrRvxv991Vyrcmw5UzM_s46k9lesZr2YfiEHsOXwz78wwtw";
    let client = TestClient::new( app_test_setup().await );

    let response = client.post("/tableCommands/dropTable").json(&TestObject::table_name()).send().await;
    assert_eq!(response.status(), 200);
    let response = client.post("/tableCommands/initTables").send().await;
    assert_eq!(response.status(), 200);

    let bearer_token_gmail = format!("Bearer {}", token_gmail);
    let bearer_token_fb = format!("Bearer {}", token_fb);
    let response = client.post("/restful/testObjects").json(&TestObjectInputParams { name: "John Doe".to_string(), age: 30 }).header("Authorization", &bearer_token_gmail).send().await;
    assert_eq!(response.status(), 200);
    let id = response.json::<i32>().await;

    let response = client.get(&format!("/restful/testObjects?id={}", id.to_string() )).header("Authorization", &bearer_token_gmail).send().await;
    assert_eq!(response.status(), 200);
    let objects: Vec<TestObject> = response.json().await;
    assert!( !objects.is_empty() );

    let response = client.get(&format!("/restful/testObjects?id={}", id.to_string() )).header("Authorization", &bearer_token_fb).send().await;
    assert_eq!(response.status(), 200);
    let objects: Vec<TestObject> = response.json().await;
    assert!( objects.is_empty() );

    let response = client.put(&format!("/restful/testObjects/{}", id.to_string() )).json(&TestObjectInputParams { name: "John Doe".to_string(), age: 29 }).header("Authorization", &bearer_token_fb).send().await;
    assert_eq!(response.status(), 400);

    let response = client.get(&format!("/restful/testObjects?id={}", id.to_string() )).header("Authorization", &bearer_token_gmail).send().await;
    assert_eq!(response.status(), 200);
    let objects: Vec<TestObject> = response.json().await;
    assert_eq!( objects[0].age, 30 );

    let response = client.put(&format!("/restful/testObjects/{}", id.to_string() )).json(&TestObjectInputParams { name: "John Doe".to_string(), age: 29 }).header("Authorization", &bearer_token_gmail).send().await;
    assert_eq!(response.status(), 200);

    let response = client.get(&format!("/restful/testObjects?id={}", id.to_string() )).header("Authorization", &bearer_token_gmail).send().await;
    assert_eq!(response.status(), 200);
    let objects: Vec<TestObject> = response.json().await;
    assert_eq!( objects[0].age, 29 );

}