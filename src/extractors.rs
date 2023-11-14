use axum::{
    async_trait,
    extract::FromRequestParts,
    http::StatusCode
};
use http::request::Parts;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation, TokenData};
use reqwest;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Jwk {
    kid: String,
    n: String,
    e: String,
}

#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

async fn fetch_jwks(jwks_url: &str) -> Result<Vec<Jwk>, reqwest::Error> {
    let resp = reqwest::get(jwks_url).await?;
    let jwks = resp.json::<Jwks>().await?;
    Ok(jwks.keys)
}

fn find_signing_key(jwks: &[Jwk], kid: &str) -> Option<DecodingKey> {
    for jwk in jwks {
        if jwk.kid == kid {
            return DecodingKey::from_rsa_components(&jwk.n, &jwk.e).ok();
        }
    }
    None
}

async fn decode_token(token: &str, audience: &str, issuer: &str) -> Result<TokenData<Claims>, StatusCode > {
    let jwks = fetch_jwks(&format!("{}{}", issuer, ".well-known/jwks.json") ).await.map_err( |_| StatusCode::INTERNAL_SERVER_ERROR )?;
    let header = jsonwebtoken::decode_header(token).map_err( |_| StatusCode::UNAUTHORIZED )?;
    let kid = header.kid.ok_or( StatusCode::UNAUTHORIZED )?;
    let decoding_key = find_signing_key(&jwks, &kid).ok_or( StatusCode::UNAUTHORIZED )?;
    
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience( &vec![ audience ] );
    validation.set_issuer(&vec![ issuer ]);
    let token_data = decode::<Claims>(token, &decoding_key, &validation).map_err( |_| StatusCode::UNAUTHORIZED )?;
    
    Ok( token_data )
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
    permissions: Vec<String>,
}

pub struct AuthUser {
    pub user_id: String,
    pub permissions: Vec<String>
}

pub struct AdminUser {
    pub user_id: String,
    pub permissions: Vec<String>
}

#[async_trait]
impl<S> FromRequestParts<S> for AuthUser where S: Send + Sync {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let auth_header = parts.headers.get("Authorization")
            .ok_or( (StatusCode::UNAUTHORIZED, "No auth token") )?;

        let mut token = auth_header.to_str().unwrap().to_string();
        let prefix = "Bearer ";
        if !token.starts_with( prefix ) {
            return Err( (StatusCode::UNAUTHORIZED, "Auth Token missing bearer") );
        } else {
            token = token.replace( prefix, "");
        }

        dotenv::dotenv().ok();

        let audience = std::env::var("AUTH0_AUDIENCE").unwrap();
        let issuer = std::env::var("AUTH0_ISSUER").unwrap();
        let decoded_token = decode_token( &token, &audience, &issuer ).await
            .map_err( |status| (status, "Error decoding token") )?;

        return Ok(
            AuthUser { user_id: decoded_token.claims.sub, permissions: decoded_token.claims.permissions }
        )
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for AdminUser where S: Send + Sync {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let auth_header = parts.headers.get("Authorization")
            .ok_or( (StatusCode::UNAUTHORIZED, "No auth token") )?;

        let mut token = auth_header.to_str().unwrap().to_string();
        let prefix = "Bearer ";
        if !token.starts_with( prefix ) {
            return Err( (StatusCode::UNAUTHORIZED, "Auth Token missing bearer") );
        } else {
            token = token.replace( prefix, "");
        }

        dotenv::dotenv().ok();

        let audience = std::env::var("AUTH0_AUDIENCE").unwrap();
        let issuer = std::env::var("AUTH0_ISSUER").unwrap();
        let decoded_token = decode_token( &token, &audience, &issuer ).await
            .map_err( |status| (status, "Error decoding token") )?;

        if !decoded_token.claims.permissions.contains( &"read:admin".to_string() ) {
            return Err( (StatusCode::UNAUTHORIZED, "invalid permissions") );
        }

        return Ok(
            AdminUser { user_id: decoded_token.claims.sub, permissions: decoded_token.claims.permissions }
        )
    }
}
