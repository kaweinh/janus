use axum::{  Extension, http::StatusCode, Json, extract::{Query, Path}};
use serde::Serialize;
use sqlx::{Row, FromRow, Result, postgres::{ PgRow, PgPool }};
use std::fmt::Debug;
use crate::{ InputSerializer, CrudConfig, EndpointVerb, ObjectPermission, SchemaTrait, KeyValue, FieldValue };
use crate::extractors::{ AuthUser, AdminUser }; 

pub async fn init_tables<S: SchemaTrait> ( 
    Extension( connection_pool ): Extension<PgPool> ) -> Result<(), StatusCode> {

    sqlx::Executor::execute( &connection_pool, S::schema().as_str() )
        .await
        .map_err(|error| {
            println!("{:?}", error);
            StatusCode::INTERNAL_SERVER_ERROR 
        })?;
    Ok(())
}

pub async fn drop_table ( 
    Extension( connection_pool ): Extension<PgPool>, 
    Json( table_name ): Json<String> ) -> Result<(), StatusCode> {

    sqlx::query( &format!("DROP TABLE IF EXISTS {} ", table_name )  )
        .execute( &connection_pool ).await
        .map_err(|error| {
            println!("{:?}", error);
            StatusCode::INTERNAL_SERVER_ERROR 
        })?;
    Ok(())
}

pub async fn reset_tables<S: SchemaTrait> ( 
    Extension( connection_pool ): Extension<PgPool> ) -> Result<(), StatusCode> {

    sqlx::query( "DROP SCHEMA public CASCADE;" )
        .execute( &connection_pool ).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?;

    sqlx::query( "CREATE SCHEMA public;" )
        .execute( &connection_pool ).await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?;  

    sqlx::Executor::execute( &connection_pool, S::schema().as_str() )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?;
    Ok(())
}

pub async fn http_get_any<T, QP>( 
    Extension( connection_pool ): Extension<PgPool>, 
    Query( parameters ): Query<QP> ) -> Result<Json<Vec<T>>, StatusCode> 
    where T: for<'r> FromRow<'r, PgRow> + Send + Unpin + CrudConfig, QP: Serialize + Debug + KeyValue + Send + Sync + Unpin {

    Ok( _http_get::<T, QP>( &connection_pool, parameters, None).await? )
}

pub async fn http_get_auth<T, QP>( 
    user: AuthUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Query( params ): Query<QP> ) -> Result<Json<Vec<T>>, StatusCode> 
    where T: for<'r> FromRow<'r, PgRow> + Send + Unpin + CrudConfig, QP: Serialize + KeyValue + Send + Sync + Unpin {

    Ok( _http_get::<T, QP>( &connection_pool, params, Some(user.user_id)).await? )
}

pub async fn http_get_admin<T, QP>( 
    user: AdminUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Query( params ): Query<QP> ) -> Result<Json<Vec<T>>, StatusCode> 
    where T: for<'r> FromRow<'r, PgRow> + Send + Unpin + CrudConfig, QP: Serialize + KeyValue + Send + Sync + Unpin {

    Ok( _http_get::<T, QP>( &connection_pool, params, Some(user.user_id)).await? )
}

async fn _http_get<T, QP>( connection_pool: &PgPool, params: QP, user_id: Option<String> ) -> Result<Json<Vec<T>>, StatusCode> 
    where T: for<'r> FromRow<'r, PgRow> + Send + Unpin + CrudConfig, QP: Serialize + KeyValue + Send + Sync + Unpin {
    
    let user_id_matched = match T::get_object_permissions( &EndpointVerb::GET ) {
        ObjectPermission::ALL => None,
        ObjectPermission::OWNER => user_id,
    };

    match T::is_custom( &EndpointVerb::GET ) {
        true => T::custom_read::<T, QP>( &connection_pool, params, user_id_matched ).await,
        false => read::<T, QP>( &connection_pool, params, user_id_matched ).await
    }
}




pub async fn http_post_any<T, UP>( 
    Extension( connection_pool ): Extension<PgPool>, 
    Json( params ): Json<UP> ) -> Result<Json<uuid::Uuid>, StatusCode> 
    where T: CrudConfig + Serialize + KeyValue + Send + Sync + Unpin, UP: InputSerializer<T> + Serialize + KeyValue  {
    
    Ok( _http_post::<T, UP>( &connection_pool, params, None ).await? )
}

pub async fn http_post_auth<T, UP>( 
    user: AuthUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Json( params ): Json<UP> ) -> Result<Json<uuid::Uuid>, StatusCode> 
    where T: CrudConfig + Serialize + KeyValue + Send + Sync + Unpin, UP: InputSerializer<T> + Serialize + KeyValue {
    
    Ok( _http_post::<T, UP>( &connection_pool, params, Some( user.user_id ) ).await? )
}

pub async fn http_post_admin<T, UP>( 
    user: AdminUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Json( params ): Json<UP> ) -> Result<Json<uuid::Uuid>, StatusCode> 
    where T: CrudConfig + Serialize + KeyValue + Send + Sync + Unpin, UP: InputSerializer<T> + Serialize + KeyValue {

    Ok( _http_post::<T, UP>( &connection_pool, params, Some( user.user_id ) ).await? )
}

async fn _http_post<T, UP>( connection_pool: &PgPool, params: UP, user_id: Option<String> ) -> Result<Json<uuid::Uuid>, StatusCode> 
    where T: CrudConfig + Serialize + KeyValue + Send + Sync + Unpin, UP: InputSerializer<T> + Serialize + KeyValue {
    let full_object = params.add_set_values(user_id);

    if params.verify() {
        match T::is_custom( &EndpointVerb::POST ) {
            true => T::custom_create::<T>( &connection_pool, full_object ).await,
            false => create::<T>( &connection_pool, full_object ).await
        }
    } else {
        Err( StatusCode::BAD_REQUEST )
    }
}



pub async fn http_put_any<T, UP>( 
    Extension( connection_pool ): Extension<PgPool>, 
    Path( id ): Path<uuid::Uuid>,
    Json( params ): Json<UP> ) -> StatusCode
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize + KeyValue + Send + Sync + Unpin {

    _http_put::<T, UP>( &connection_pool, id, params, None ).await
}

pub async fn http_put_auth<T, UP>( 
    user: AuthUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Path( id ): Path<uuid::Uuid>,
    Json( params ): Json<UP> ) -> StatusCode
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize + KeyValue + Send + Sync + Unpin {

    _http_put::<T, UP>( &connection_pool, id, params, Some( user.user_id ) ).await
}

pub async fn http_put_admin<T, UP>( 
    user: AdminUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Path( id ): Path<uuid::Uuid>,
    Json( params ): Json<UP> ) -> StatusCode
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize + KeyValue + Send + Sync + Unpin {

    _http_put::<T, UP>( &connection_pool, id, params, Some( user.user_id ) ).await
}

async fn _http_put<T, UP>( connection_pool: &PgPool, id: uuid::Uuid, params: UP, user_id: Option<String> ) -> StatusCode
    where T: CrudConfig + Serialize, UP: InputSerializer<T> + Serialize + KeyValue + Send + Sync + Unpin {

    let user_id_matched = match T::get_object_permissions( &EndpointVerb::PUT ) {
        ObjectPermission::ALL => None,
        ObjectPermission::OWNER => user_id,
    };

    if params.verify() {
        match T::is_custom( &EndpointVerb::PUT ) {
            true => T::custom_update::<UP>( &connection_pool, id, params, user_id_matched ).await,
            false => update::<T, UP>( &connection_pool, id, params, user_id_matched ).await
        }
    } else {
        StatusCode::BAD_REQUEST
    }
}




pub async fn http_delete_any<T>( 
    Extension( connection_pool ): Extension<PgPool>, 
    Path( id ): Path<uuid::Uuid> ) -> StatusCode where T: CrudConfig {
    
    _http_delete::<T>( &connection_pool, id, None ).await
}

pub async fn http_delete_auth<T>( 
    user: AuthUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Path( id ): Path<uuid::Uuid> ) -> StatusCode where T: CrudConfig {
    
    _http_delete::<T>( &connection_pool, id, Some( user.user_id ) ).await
}

pub async fn http_delete_admin<T>( 
    user: AdminUser,
    Extension( connection_pool ): Extension<PgPool>, 
    Path( id ): Path<uuid::Uuid>,) -> StatusCode where T: CrudConfig {
    
    _http_delete::<T>( &connection_pool, id, Some( user.user_id ) ).await
}

async fn _http_delete<T>( connection_pool: &PgPool, id: uuid::Uuid, user_id: Option<String> ) -> StatusCode where T: CrudConfig {
    let ids = vec![id];

    let user_id_matched = match T::get_object_permissions( &EndpointVerb::DELETE ) {
        ObjectPermission::ALL => None,
        ObjectPermission::OWNER => user_id,
    };

    match T::is_custom( &EndpointVerb::DELETE ) {
        true => T::custom_delete( &connection_pool, ids, user_id_matched ).await,
        false => delete::<T>( &connection_pool, ids, user_id_matched ).await
    }
}



async fn read<T, QP>( connection_pool: &PgPool, filters: QP, user_id: Option<String> ) -> Result <Json<Vec<T>>, StatusCode> where T: for<'r> FromRow<'r, PgRow> + Send + Unpin + CrudConfig, QP: Serialize + KeyValue {
    let mut query = format!("SELECT * FROM {} WHERE ", T::table_name() );
    let mut count = 1;
    let mut order_col = "id";
    let mut order_dir = "DESC";

    if let Some(_) = user_id {
        query = query + "user_id = $" + &count.to_string() + " AND ";
        count += 1;
    }

    let mut bindings: Vec<FieldValue> = vec![]; 
    for (key, value ) in filters.key_value_pairs() {
        if key == "order_by" {
            if let FieldValue::STRING(value) = value {
                order_col = value;
            }
            continue;
        }

        if key == "order_dir" {
            if let FieldValue::STRING(value) = value {
                match value.as_str() {
                    "asc" => { order_dir = "ASC"; },
                    _ => {}
                }
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

        bindings.push( value );
        count += 1;
    }
    query = query.chars().take(query.len() - 5).collect();
    query = query + " ORDER BY " + &order_col + " " + &order_dir;

    let mut q = sqlx::query_as::<_, T>( &query );
    if let Some(user_id) = user_id {
        q = q.bind(user_id);
    }

    for binding in bindings {
        match binding {
            FieldValue::UUID( value ) => { q = q.bind( value ); },
            FieldValue::STRING( value ) => { q = q.bind( value ); },
            FieldValue::INTEGER( value ) => { q = q.bind( value ); },
            FieldValue::DATE( value ) => { q = q.bind( value ); },
            FieldValue::BOOLEAN( value ) => { q = q.bind( value ); },
            FieldValue::FLOAT( value ) => { q = q.bind( value ); }
        }
    }

    let rows = q.fetch_all(connection_pool).await
                        .map_err(|error| {
                            println!("{:?}", error);
                            StatusCode::INTERNAL_SERVER_ERROR 
                        })?;

    Ok( Json( rows ) )
}

async fn create<T: CrudConfig + KeyValue>( connection_pool: &PgPool, values: T ) -> Result <Json<uuid::Uuid>, StatusCode> {
    let mut query_part1 = format!( "INSERT INTO {} (" , T::table_name() );
    let mut query_part2 = " ) VALUES ( ".to_string();
    let mut count = 0;
    for (key, _) in values.key_value_pairs() {
        query_part1 = query_part1 + key + ", ";
        count += 1;
        query_part2 = query_part2 + "$" + &count.to_string() + ", ";
    }
    query_part1 = query_part1.chars().take(query_part1.len() - 2).collect();
    query_part2 = query_part2.chars().take(query_part2.len() - 2).collect();
    let query = format!("{}{} ) RETURNING id", query_part1, query_part2);

    let mut q = sqlx::query( &query );
    for (_, value) in values.key_value_pairs() {
        match value {
            FieldValue::UUID( value ) => { q = q.bind( value ); },
            FieldValue::STRING( value ) => { q = q.bind( value ); },
            FieldValue::INTEGER( value ) => { q = q.bind( value ); },
            FieldValue::DATE( value ) => { q = q.bind( value ); },
            FieldValue::BOOLEAN( value ) => { q = q.bind( value ); },
            FieldValue::FLOAT( value ) => { q = q.bind( value ); }
        }
    }

    let id = q.fetch_one(connection_pool).await
                    .map_err(|error| {
                        println!("{:?}", error);
                        StatusCode::INTERNAL_SERVER_ERROR 
                    })?
                    .try_get::<uuid::Uuid, _>(0)
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR )?;
    
    Ok( Json(id) )
}

async fn update<T: CrudConfig, UP: KeyValue>( connection_pool: &PgPool, id: uuid::Uuid, values: UP, user_id: Option<String> ) -> StatusCode {
    let mut query = format!( "UPDATE {} SET " , T::table_name() );
    let mut count = 0;
    for (key, _) in values.key_value_pairs() {
        count += 1;
        query = query + key + " = $" + &count.to_string() + ", ";
    }
    query = query.chars().take(query.len() - 2).collect();
    query = query + &format!(" WHERE id = ${}", &( count + 1 ).to_string());

    if let Some(_) = user_id {
        query = query + " AND user_id = $" + &( count + 2 ).to_string();
    }

    let mut q = sqlx::query( &query );
    for (_, value) in values.key_value_pairs() {
        match value {
            FieldValue::UUID( value ) => { q = q.bind( value ); },
            FieldValue::STRING( value ) => { q = q.bind( value ); },
            FieldValue::INTEGER( value ) => { q = q.bind( value ); },
            FieldValue::DATE( value ) => { q = q.bind( value ); },
            FieldValue::BOOLEAN( value ) => { q = q.bind( value ); },
            FieldValue::FLOAT( value ) => { q = q.bind( value ); }
        }
    }

    q = q.bind(id);
    if let Some(user_id) = user_id {
        q = q.bind(user_id);
    }
    q.execute(connection_pool)
        .await
        .map_err(|error| {
            println!("{:?}", error);
            StatusCode::INTERNAL_SERVER_ERROR 
        })
        .map(|resp| {
            if resp.rows_affected() == 0 {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::OK
            }
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR ).unwrap()
}

async fn delete<T: CrudConfig>( connection_pool: &PgPool, ids: Vec<uuid::Uuid>, user_id: Option<String> ) -> StatusCode {
    let mut query = format!( "DELETE FROM {} WHERE id IN (" , T::table_name() );
    let mut count = 0;
    for _ in &ids {
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
        .map_err(|error| {
            println!("{:?}", error);
            StatusCode::INTERNAL_SERVER_ERROR 
        })
        .map(|resp| {
            if resp.rows_affected() == 0 {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::OK
            }
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR ).unwrap()
}