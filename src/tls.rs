use std::fs;
use std::path::Path;

use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::error::{Error, Result};
use crate::options::{
    env_data_dir, env_insecure, env_server_name, Options, CA_FILE, CERT_FILE, KEY_FILE,
};

pub(crate) async fn open_channel(opts: &Options, addr: &str) -> Result<Channel> {
    let insecure = opts.insecure || (env_insecure() && opts.data_dir.is_none());
    if insecure {
        let uri = format!("http://{addr}");
        return Endpoint::from_shared(uri)
            .map_err(|e| Error::new(format!("clusdr: dial {addr}: {e}")))?
            .connect()
            .await
            .map_err(|e| Error::new(format!("clusdr: dial {addr}: {e}")));
    }

    let dir = opts.data_dir.clone().unwrap_or_else(env_data_dir);
    let (ca, cert, key) = load_pems(&dir)?;
    let server_name = if !opts.server_name.is_empty() {
        opts.server_name.clone()
    } else {
        let env = env_server_name();
        if !env.is_empty() {
            env
        } else {
            cn_from_pem(&cert).ok_or_else(|| {
                Error::new(
                    "clusdr: TLS hostname unknown; set CLUSDR_TLS_SERVER_NAME or Options::server_name to the peer node id",
                )
            })?
        }
    };

    let tls = ClientTlsConfig::new()
        .domain_name(server_name)
        .ca_certificate(Certificate::from_pem(ca))
        .identity(Identity::from_pem(cert, key));
    let uri = format!("https://{addr}");
    Endpoint::from_shared(uri)
        .map_err(|e| Error::new(format!("clusdr: dial {addr}: {e}")))?
        .tls_config(tls)
        .map_err(|e| Error::new(format!("clusdr: tls: {e}")))?
        .connect()
        .await
        .map_err(|e| Error::new(format!("clusdr: dial {addr}: {e}")))
}

fn load_pems(dir: &Path) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let ca = dir.join(CA_FILE);
    let cert = dir.join(CERT_FILE);
    let key = dir.join(KEY_FILE);
    if !(ca.is_file() && cert.is_file() && key.is_file()) {
        return Err(Error::new(format!(
            "clusdr: TLS enabled but {CA_FILE}/{CERT_FILE}/{KEY_FILE} missing in {}; set CLUSDR_TLS=disabled or Options::insecure(true)",
            dir.display()
        )));
    }
    Ok((
        fs::read(&ca).map_err(|e| Error::new(format!("clusdr: read {}: {e}", ca.display())))?,
        fs::read(&cert).map_err(|e| Error::new(format!("clusdr: read {}: {e}", cert.display())))?,
        fs::read(&key).map_err(|e| Error::new(format!("clusdr: read {}: {e}", key.display())))?,
    ))
}

fn cn_from_pem(pem: &[u8]) -> Option<String> {
    let (_, doc) = x509_parser::pem::parse_x509_pem(pem).ok()?;
    let (_, cert) = x509_parser::parse_x509_certificate(&doc.contents).ok()?;
    let cn = cert
        .subject()
        .iter_common_name()
        .next()?
        .as_str()
        .ok()?
        .to_owned();
    Some(cn)
}
