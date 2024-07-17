use axum::{
    async_trait,
    extract::FromRequestParts,
    http::StatusCode
};
use axum::http::request::Parts;
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

async fn decode_auth0_token(token: &str, audience: &str, issuer: &str) -> Result<TokenData<Auth0Claims>, StatusCode > {
    let jwks = fetch_jwks(&format!("{}{}", issuer, ".well-known/jwks.json") ).await.map_err( | err | { println!("{:?}", err ); StatusCode::INTERNAL_SERVER_ERROR } )?;
    let header = jsonwebtoken::decode_header(token).map_err( |_| StatusCode::UNAUTHORIZED )?;
    let kid = header.kid.ok_or( StatusCode::UNAUTHORIZED )?;
    let decoding_key = find_signing_key(&jwks, &kid).ok_or( StatusCode::UNAUTHORIZED )?;
    
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience( &vec![ audience ] );
    validation.set_issuer(&vec![ issuer ]);
    let token_data = decode::<Auth0Claims>(token, &decoding_key, &validation).map_err( |_| StatusCode::UNAUTHORIZED )?;
    //println!("{:?}", token_data.claims);
    
    Ok( token_data )
}

async fn decode_clerk_token(token: &str, audience: &str, issuer: &str) -> Result<TokenData<ClerkClaims>, StatusCode > {
    let jwks = fetch_jwks(&format!("{}{}", issuer, "/.well-known/jwks.json") ).await.map_err( | err | { println!("{:?}", err ); StatusCode::INTERNAL_SERVER_ERROR } )?;
    let header = jsonwebtoken::decode_header(token).map_err( | err | { println!("{:?}", err ); StatusCode::UNAUTHORIZED } )?;
    let kid = header.kid.ok_or( StatusCode::UNAUTHORIZED )?;
    let decoding_key = find_signing_key(&jwks, &kid).ok_or( StatusCode::UNAUTHORIZED )?;
    
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience( &vec![ audience ] );
    validation.set_issuer(&vec![ issuer ]);
    let token_data = decode::<ClerkClaims>(token, &decoding_key, &validation).map_err( | err | { println!("{:?}", err ); StatusCode::UNAUTHORIZED }  )?;
    //println!("{:?}", token_data.claims);
    
    Ok( token_data )
}


async fn decode_custom_token( token: &str, audience: &str, issuer: &str, secret: &str ) -> Result<TokenData<CustomClaims>, StatusCode > {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_audience( &vec![ audience ] );
    validation.set_issuer(&vec![ issuer ]);
    let token_data = decode::<CustomClaims>(token, &DecodingKey::from_secret( secret.as_bytes() ), &validation).map_err( | err | { println!("{:?}", err ); StatusCode::UNAUTHORIZED }  )?;
    //println!("{:?}", token_data.claims);
    
    Ok( token_data )
}

#[derive(Debug, Deserialize)]
struct Auth0Claims {
    sub: String,
    subscription: String,
    permissions: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ClerkClaims {
    sub: String
}

#[derive(Debug, Deserialize)]
struct CustomClaims {
    subscription: String,
    clerk_id: String
}

pub struct AuthUser {
    pub user_id: String,
    pub subscription: String
}

pub struct AdminUser {
    pub user_id: String,
    pub subscription: String,
    pub permissions: Vec<String>
}

async fn pull_header_token( parts: &mut Parts, key: &str ) -> Result<String, (StatusCode, &'static str) > {
    let auth_header = parts.headers.get( key )
        .ok_or( (StatusCode::UNAUTHORIZED, "No auth token") )?;

    let mut token = auth_header.to_str().unwrap().to_string();
    let prefix = "Bearer ";
    if !token.starts_with( prefix ) {
        return Err( (StatusCode::UNAUTHORIZED, "Auth Token missing bearer") );
    } else {
        token = token.replace( prefix, "");
    }

    Ok( token )
}

#[async_trait]
impl<S> FromRequestParts<S> for AuthUser where S: Send + Sync {
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        let access_token = pull_header_token( parts, "Authorization" ).await?;

        let mut subscription_token = "none".to_string();
        if parts.headers.contains_key("Subscription") {
            subscription_token = pull_header_token( parts, "Subscription" ).await?;
        }

        dotenv::dotenv().ok();

        //println!("{:?}", token);

        let audience = std::env::var("JWT_AUDIENCE").unwrap();
        let clerk_issuer = std::env::var("CLERK_ISSUER").unwrap();
        let jwt_issuer = std::env::var("JWT_ISSUER").unwrap();
        let secret = std::env::var("JWT_SECRET").unwrap();
        let decoded_access_token = decode_clerk_token( &access_token, &audience, &clerk_issuer ).await
            .map_err( |status| (status, "Error decoding auth token") )?;

        if subscription_token == "none" {
            return Ok(
                AuthUser { user_id: decoded_access_token.claims.sub, subscription: subscription_token }
            )
        }

        let decoded_sub_token = decode_custom_token( &subscription_token, &audience, &jwt_issuer, &secret ).await
            .map_err( |status| (status, "Error decoding sub token") )?;

        if decoded_access_token.claims.sub != decoded_sub_token.claims.clerk_id {
            return Err( (StatusCode::UNAUTHORIZED, "invalid permissions") );
        }

        return Ok(
            AuthUser { user_id: decoded_access_token.claims.sub, subscription: decoded_sub_token.claims.subscription }
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
        let decoded_token = decode_auth0_token( &token, &audience, &issuer ).await
            .map_err( |status| (status, "Error decoding token") )?;

        if !decoded_token.claims.permissions.contains( &"read:admin".to_string() ) {
            return Err( (StatusCode::UNAUTHORIZED, "invalid permissions") );
        }

        return Ok(
            AdminUser { user_id: decoded_token.claims.sub, permissions: decoded_token.claims.permissions, subscription: decoded_token.claims.subscription  }
        )
    }
}
