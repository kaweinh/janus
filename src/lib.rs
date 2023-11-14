use std::fmt::Debug;
use axum::Router;
use serde::{Serialize, Deserialize};
use sqlx::{FromRow, postgres::{PgRow, PgConnectOptions, PgPool}};
use http::StatusCode;
use std::collections::HashMap;
use serde_json::Value;
use axum::async_trait;
use axum::Json;

pub mod extractors;
mod endpoints;

mod tests;

#[derive(Debug, Clone)]
pub enum EndpointVerb {
    GET,
    POST,
    PUT,
    DELETE
} 

pub enum ObjectPermission {
    ALL,
    OWNER
}

pub enum AccessPermission {
    ANY,
    AUTHENTICATED,
    ADMIN
}

pub trait InputSerializer<T: CrudConfig + Serialize > {
    fn verify(&self) -> bool;
    fn add_set_values(&self, user_id: Option<String>) -> T;
}

#[async_trait]
pub trait CrudConfig {
    fn table_name() -> &'static str;
    fn endpoint_name() -> &'static str;
    fn schema() -> &'static str;

    fn include_endpoint( verb: &EndpointVerb ) -> bool;
    fn is_custom( verb: &EndpointVerb ) -> bool;
    fn get_object_permissions( verb: &EndpointVerb ) -> ObjectPermission;
    fn get_access_permissions( verb: &EndpointVerb ) -> AccessPermission;

    #[allow(unused_variables)]
    async fn custom_create( connection_pool: &PgPool, table_name: &str, values: &HashMap<String, Value> ) -> Result<Json<i32>, StatusCode> { 
        Err( StatusCode::NOT_IMPLEMENTED ) 
    } 

    #[allow(unused_variables)]
    async fn custom_read<T>( connection_pool: &PgPool, table_name: &str, filters: &HashMap<String, Value>, user_id: Option<String> ) -> Result<Json<Vec<T>>, StatusCode> where T: for<'r> FromRow<'r, PgRow> + Send + Unpin {
        Err( StatusCode::NOT_IMPLEMENTED )
    }

    #[allow(unused_variables)]
    async fn custom_update( connection_pool: &PgPool, table_name: &str, id: i32, values: &HashMap<String, Value>, user_id: Option<String> ) -> StatusCode {
        StatusCode::NOT_IMPLEMENTED
    }

    #[allow(unused_variables)]
    async fn custom_delete( connection_pool: &PgPool, table_name: &str, ids: &Vec<i32>, user_id: Option<String> ) -> StatusCode {
        StatusCode::NOT_IMPLEMENTED
    }
}

pub fn create_endpoint_router<T, UP, QP>() -> Router where 
        T: for<'r> FromRow<'r, PgRow> + Send + Sync + 'static + Unpin + CrudConfig + Serialize, 
        UP: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static + InputSerializer<T>, 
        QP: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static + Debug {
    let mut router = Router::new();

    for verb in vec![ EndpointVerb::GET, EndpointVerb::POST, EndpointVerb::PUT, EndpointVerb::DELETE ] {
        if T::include_endpoint( &verb ) {
            match verb {
                EndpointVerb::GET => {
                    match T::get_access_permissions( &verb ) {
                        AccessPermission::ANY => { router = router.route( &format!("/{}", T::endpoint_name()), axum::routing::get( endpoints::http_get_any::<T, QP> ) ); },
                        AccessPermission::AUTHENTICATED => { router = router.route( &format!("/{}", T::endpoint_name()), axum::routing::get( endpoints::http_get_auth::<T, QP> ) ); },
                        AccessPermission::ADMIN => { router = router.route( &format!("/{}", T::endpoint_name()), axum::routing::get( endpoints::http_get_admin::<T, QP> ) ); }
                    }
                },
                EndpointVerb::POST => {
                    match T::get_access_permissions( &verb ) {
                        AccessPermission::ANY => { router = router.route( &format!("/{}", T::endpoint_name()), axum::routing::post( endpoints::http_post_any::<T, UP> ) ); },
                        AccessPermission::AUTHENTICATED => { router = router.route( &format!("/{}", T::endpoint_name()), axum::routing::post( endpoints::http_post_auth::<T, UP> ) ); },
                        AccessPermission::ADMIN => { router = router.route( &format!("/{}", T::endpoint_name()), axum::routing::post( endpoints::http_post_admin::<T, UP> ) ); }
                    }
                },
                EndpointVerb::PUT => {
                    match T::get_access_permissions( &verb ) {
                        AccessPermission::ANY => { router = router.route( &format!("/{}/:id", T::endpoint_name()), axum::routing::put( endpoints::http_put_any::<T, UP> ) ); },
                        AccessPermission::AUTHENTICATED => { router = router.route( &format!("/{}/:id", T::endpoint_name()), axum::routing::put( endpoints::http_put_auth::<T, UP> ) ); },
                        AccessPermission::ADMIN => { router = router.route( &format!("/{}/:id", T::endpoint_name()), axum::routing::put( endpoints::http_put_admin::<T, UP> ) ); }
                    }
                },
                EndpointVerb::DELETE => {
                    match T::get_access_permissions( &verb ) {
                        AccessPermission::ANY => { router = router.route( &format!("/{}/:id", T::endpoint_name()), axum::routing::delete( endpoints::http_delete_any::<T> ) ); },
                        AccessPermission::AUTHENTICATED => { router = router.route( &format!("/{}/:id", T::endpoint_name()), axum::routing::delete( endpoints::http_delete_auth::<T> ) ); },
                        AccessPermission::ADMIN => { router = router.route( &format!("/{}/:id", T::endpoint_name()), axum::routing::delete( endpoints::http_delete_admin::<T> ) ); }
                    }
                }
            }
        }
    }

    return router;
}

pub fn create_tables_router() -> Router {
    let mut router = Router::new();

    router = router
                .route( "/initTables", axum::routing::post( endpoints::init_tables ) )
                .route( "/resetTables", axum::routing::post( endpoints::reset_tables ) )
                .route( "/dropTable", axum::routing::post( endpoints::drop_table ) );

    return router;
}

pub async fn connect_to_db( host: &str, port: u16, username: &str, password: &str, db_name: &str ) -> anyhow::Result<PgPool> {
    let pool_options = PgConnectOptions::new()
        .host(host)
        .port(port)
        .username(username)
        .password(password)
        .database(db_name);

    let connection_pool: PgPool = PgPool::connect_with( pool_options ).await?;
    Ok( connection_pool )
}
