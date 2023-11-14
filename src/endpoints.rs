use axum::{  Extension, http::StatusCode, Json, extract::{Query, Path}};
use serde_json::Value;
use serde::Serialize;
use sqlx::{Row, FromRow, Result, postgres::{ PgRow, PgPool }};
use std::{collections::HashMap, fmt::Debug};
use crate::InputSerializer;
use crate::CrudConfig;
use crate::extractors::{ AuthUser, AdminUser };
use crate::EndpointVerb;
use crate::ObjectPermission;

fn convert_to_hashmap<T>( object: &T ) -> HashMap<String, Value> where T: Serialize {
    let json_string = serde_json::to_string(&object).unwrap();
    let map: HashMap<String, Value> = serde_json::from_str(&json_string).unwrap();
    return map
}

pub async fn init_tables ( 
    Extension( connection_pool ): Extension<PgPool>, 
    Json( schema ): Json<String> ) -> Result<(), StatusCode> {

    sqlx::query( schema.as_str() )
        .execute( &connection_pool ).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?;
    Ok(())
}

pub async fn drop_table ( 
    Extension( connection_pool ): Extension<PgPool>, 
    Json( table_name ): Json<String> ) -> Result<(), StatusCode> {

    sqlx::query( &format!("DROP TABLE IF EXISTS {} ", table_name )  )
        .execute( &connection_pool ).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?;
    Ok(())
}

pub async fn reset_tables ( 
    Extension( connection_pool ): Extension<PgPool>, 
    Json( schema ): Json<String> ) -> Result<(), StatusCode> {

    sqlx::query( "DROP SCHEMA public CASCADE;" )
        .execute( &connection_pool ).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?;

    sqlx::query( "CREATE SCHEMA public;" )
        .execute( &connection_pool ).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?;  

    sqlx::query( schema.as_str() )
        .execute( &connection_pool ).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?;
    Ok(())
}

pub async fn http_get_any<T, QP>( 
    Extension( connection_pool ): Extension<PgPool>, 
    Query( parameters ): Query<QP> ) -> Result<Json<Vec<T>>, StatusCode> 
    where T: for<'r> FromRow<'r, PgRow> + Send + Unpin + CrudConfig, QP: Serialize + Debug {

    Ok( _http_get::<T, QP>( &connection_pool, parameters, None).await? )
}

pub async fn http_get_auth<T, QP>( 
    user: AuthUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Query( params ): Query<QP> ) -> Result<Json<Vec<T>>, StatusCode> 
    where T: for<'r> FromRow<'r, PgRow> + Send + Unpin + CrudConfig, QP: Serialize {

    Ok( _http_get::<T, QP>( &connection_pool, params, Some(user.user_id)).await? )
}

pub async fn http_get_admin<T, QP>( 
    user: AdminUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Query( params ): Query<QP> ) -> Result<Json<Vec<T>>, StatusCode> 
    where T: for<'r> FromRow<'r, PgRow> + Send + Unpin + CrudConfig, QP: Serialize {

    Ok( _http_get::<T, QP>( &connection_pool, params, Some(user.user_id)).await? )
}

async fn _http_get<T, QP>( connection_pool: &PgPool, params: QP, user_id: Option<String> ) -> Result<Json<Vec<T>>, StatusCode> 
    where T: for<'r> FromRow<'r, PgRow> + Send + Unpin + CrudConfig, QP: Serialize {
    let map = convert_to_hashmap( &params );
    
    let user_id_matched = match T::get_object_permissions( &EndpointVerb::GET ) {
        ObjectPermission::ALL => None,
        ObjectPermission::OWNER => user_id,
    };

    match T::is_custom( &EndpointVerb::GET ) {
        true => T::custom_read::<T>( &connection_pool, T::table_name(), &map, user_id_matched ).await,
        false => read::<T>( &connection_pool, T::table_name(), &map, user_id_matched ).await
    }
}




pub async fn http_post_any<T, UP>( 
    Extension( connection_pool ): Extension<PgPool>, 
    Json( params ): Json<UP> ) -> Result<Json<i32>, StatusCode> 
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize {
    
    Ok( _http_post::<T, UP>( &connection_pool, params, None ).await? )
}

pub async fn http_post_auth<T, UP>( 
    user: AuthUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Json( params ): Json<UP> ) -> Result<Json<i32>, StatusCode> 
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize {
    
    Ok( _http_post::<T, UP>( &connection_pool, params, Some( user.user_id ) ).await? )
}

pub async fn http_post_admin<T, UP>( 
    user: AdminUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Json( params ): Json<UP> ) -> Result<Json<i32>, StatusCode> 
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize {

    Ok( _http_post::<T, UP>( &connection_pool, params, Some( user.user_id ) ).await? )
}

async fn _http_post<T, UP>( connection_pool: &PgPool, params: UP, user_id: Option<String> ) -> Result<Json<i32>, StatusCode> 
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize {
    let full_object = params.add_set_values(user_id);

    let map = convert_to_hashmap( &full_object );

    if params.verify() {
        match T::is_custom( &EndpointVerb::POST ) {
            true => T::custom_create( &connection_pool, T::table_name(), &map ).await,
            false => create( &connection_pool, T::table_name(), &map ).await
        }
    } else {
        Err( StatusCode::BAD_REQUEST )
    }
}



pub async fn http_put_any<T, UP>( 
    Extension( connection_pool ): Extension<PgPool>, 
    Path( id ): Path<i32>,
    Json( params ): Json<UP> ) -> StatusCode
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize {

    _http_put::<T, UP>( &connection_pool, id, params, None ).await
}

pub async fn http_put_auth<T, UP>( 
    user: AuthUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Path( id ): Path<i32>,
    Json( params ): Json<UP> ) -> StatusCode
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize {

    _http_put::<T, UP>( &connection_pool, id, params, Some( user.user_id ) ).await
}

pub async fn http_put_admin<T, UP>( 
    user: AdminUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Path( id ): Path<i32>,
    Json( params ): Json<UP> ) -> StatusCode
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize {

    _http_put::<T, UP>( &connection_pool, id, params, Some( user.user_id ) ).await
}

async fn _http_put<T, UP>( connection_pool: &PgPool, id: i32, params: UP, user_id: Option<String> ) -> StatusCode
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize {
    let map = convert_to_hashmap( &params );

    let user_id_matched = match T::get_object_permissions( &EndpointVerb::PUT ) {
        ObjectPermission::ALL => None,
        ObjectPermission::OWNER => user_id,
    };

    if params.verify() {
        match T::is_custom( &EndpointVerb::PUT ) {
            true => T::custom_update( &connection_pool, T::table_name(), id, &map, user_id_matched ).await,
            false => update( &connection_pool, T::table_name(), id, &map, user_id_matched ).await
        }
    } else {
        StatusCode::BAD_REQUEST
    }
}




pub async fn http_delete_any<T>( 
    Extension( connection_pool ): Extension<PgPool>, 
    Path(id): Path<i32> ) -> StatusCode where T: CrudConfig {
    
    _http_delete::<T>( &connection_pool, id, None ).await
}

pub async fn http_delete_auth<T>( 
    user: AuthUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Path(id): Path<i32> ) -> StatusCode where T: CrudConfig {
    
    _http_delete::<T>( &connection_pool, id, Some( user.user_id ) ).await
}

pub async fn http_delete_admin<T>( 
    user: AdminUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Path(id): Path<i32> ) -> StatusCode where T: CrudConfig {
    
    _http_delete::<T>( &connection_pool, id, Some( user.user_id ) ).await
}

async fn _http_delete<T>( connection_pool: &PgPool, id: i32, user_id: Option<String> ) -> StatusCode where T: CrudConfig {
    let ids = vec![id];

    let user_id_matched = match T::get_object_permissions( &EndpointVerb::DELETE ) {
        ObjectPermission::ALL => None,
        ObjectPermission::OWNER => user_id,
    };

    match T::is_custom( &EndpointVerb::DELETE ) {
        true => T::custom_delete( &connection_pool, T::table_name(), &ids, user_id_matched ).await,
        false => delete( &connection_pool, T::table_name(), &ids, user_id_matched ).await
    }
}



async fn read<T>( connection_pool: &PgPool, table_name: &str, filters: &HashMap<String, Value>, user_id: Option<String> ) -> Result <Json<Vec<T>>, StatusCode> where T: for<'r> FromRow<'r, PgRow> + Send + Unpin + CrudConfig {
    let mut query = format!("SELECT * FROM {} WHERE ", table_name );
    let mut count = 1;
    let mut order_col = "id".to_string();
    let mut order_dir = "DESC".to_string();

    if let Some(_) = user_id {
        query = query + "user_id = $" + &count.to_string() + " AND ";
        count += 1;
    }

    for (key, value ) in filters {
        if value.is_null() {
            continue;
        }

        if key == "order_by" {
            order_col = value.as_str().unwrap().to_string();
            continue;
        }

        if key == "order_dir" {
            match value.as_str().unwrap() {
                "asc" => { order_dir = "ASC".to_string(); },
                _ => {}
            }
            continue;
        }

        if key.len() < 3 {
            query = query + &key + " = $" + &count.to_string() + " AND ";
        } else {
            match &key[key.len() - 3..] {
                "_gt" => { query = query + &key[0..key.len() - 3] + " > $" + &count.to_string() + " AND "; },
                "_lt" => { query = query + &key[0..key.len() - 3] + " < $" + &count.to_string() + " AND "; },
                "_ge" => { query = query + &key[0..key.len() - 3] + " >= $" + &count.to_string() + " AND "; },
                "_le" => { query = query + &key[0..key.len() - 3] + " <= $" + &count.to_string() + " AND "; },
                _ => { query = query + &key + " = $" + &count.to_string() + " AND "; }
            }
        }
        count += 1;
    }
    query = query.chars().take(query.len() - 5).collect();
    query = query + " ORDER BY " + &order_col + " " + &order_dir;

    let mut q = sqlx::query_as::<_, T>( &query );
    if let Some(user_id) = user_id {
        q = q.bind(user_id);
    }

    for (_, value) in filters {
        if value.is_null() {
            continue;
        }

        if value.is_string() {
            q = q.bind( value.as_str().unwrap() );
        } else if value.is_i64() {
            q = q.bind( value.as_i64().unwrap() );
        } else if value.is_f64() {
            q = q.bind( value.as_f64().unwrap() );
        } else {
            return Err( StatusCode::BAD_REQUEST) ;
        } 
    }

    let rows = q.fetch_all(connection_pool).await
                                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?;

    Ok( Json( rows ) )
}

async fn create( connection_pool: &PgPool, table_name: &str, values: &HashMap<String, Value> ) -> Result <Json<i32>, StatusCode> {
    let mut query = format!( "INSERT INTO {} (" , table_name );
    let mut count = 0;
    for (key, _) in values {
        if key == "id" {
            continue;
        }
        query = query + key + ", ";
    }
    query = query.chars().take(query.len() - 2).collect();
    query = query + ") VALUES (";
    for (key, _) in values {
        if key == "id" {
            continue;
        }
        count += 1;
        query = query + "$" + &count.to_string() + ", ";
    }
    query = query.chars().take(query.len() - 2).collect();
    query = query + ") RETURNING id";

    let mut q = sqlx::query( &query );
    for (key, value) in values {
        if key == "id" {
            continue;
        }

        if value.is_string() {
            q = q.bind( value.as_str().unwrap() );
        } else if value.is_i64() {
            q = q.bind( value.as_i64().unwrap() );
        } else if value.is_f64() {
            q = q.bind( value.as_f64().unwrap() );
        } else {
            return Err( StatusCode::BAD_REQUEST) ;
        } 
    }

    let id = q.fetch_one(connection_pool).await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?
                    .try_get::<i32, _>(0)
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?;
    
    Ok( Json(id) )
}

async fn update( connection_pool: &PgPool, table_name: &str, id: i32, values: &HashMap<String, Value>, user_id: Option<String> ) -> StatusCode {
    let mut query = format!( "UPDATE {} SET " , table_name );
    let mut count = 0;
    for (key, _) in values {
        count += 1;
        query = query + key + " = $" + &count.to_string() + ", ";
    }
    query = query.chars().take(query.len() - 2).collect();
    query = query + &format!(" WHERE id = ${}", &( count + 1 ).to_string());

    if let Some(_) = user_id {
        query = query + " AND user_id = $" + &( count + 2 ).to_string();
    }

    let mut q = sqlx::query( &query );
    for (_, value) in values {

        if value.is_string() {
            q = q.bind( value.as_str().unwrap() );
        } else if value.is_i64() {
            q = q.bind( value.as_i64().unwrap() );
        } else if value.is_f64() {
            q = q.bind( value.as_f64().unwrap() );
        } else {
            return StatusCode::BAD_REQUEST ;
        } 
    }

    q = q.bind(id);
    if let Some(user_id) = user_id {
        q = q.bind(user_id);
    }
    q.execute(connection_pool)
        .await
        .map(|resp| {
            if resp.rows_affected() == 0 {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::OK
            }
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR ).unwrap()
}

async fn delete( connection_pool: &PgPool, table_name: &str, ids: &Vec<i32>, user_id: Option<String> ) -> StatusCode {
    let mut query = format!( "DELETE FROM {} WHERE id IN (" , table_name );
    let mut count = 0;
    for _ in ids {
        count += 1;
        query = query + "$" + &count.to_string() + ", ";
    }
    query = query.chars().take(query.len() - 2).collect();
    query = query + ")";

    if let Some(_) = user_id {
        query = query + " AND user_id = $" + &( count + 1 ).to_string();
    }

    let mut q = sqlx::query( &query );
    for id in ids {
        q = q.bind(id);
    }
    if let Some(user_id) = user_id {
        q = q.bind(user_id);
    }

    q.execute(connection_pool)
        .await
        .map(|resp| {
            if resp.rows_affected() == 0 {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::OK
            }
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR ).unwrap()
}