use super::*;
#[tokio::test]
#[cfg(all(feature = "warehouse-adx", feature = "auth-azure"))]
async fn saved_kusto_and_storage_scopes_refuse_foreign_audiences_before_cli_authentication() {
    let kusto =
        ScopedAzure { scope: BearerScope::Kusto("https://cluster.westeurope.kusto.windows.net".into()), tenant: None };
    let storage =
        ScopedAzure { scope: BearerScope::Storage, tenant: Some("aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb".into()) };
    for resource in [
        "https://foreign.invalid",
        "https://other.kusto.windows.net",
        "https://storage.azure.com/",
        "http://cluster.westeurope.kusto.windows.net",
    ] {
        assert!(kusto.bearer_token(resource).await.is_err());
    }
    for resource in
        ["https://foreign.invalid", "https://cluster.westeurope.kusto.windows.net", "https://management.azure.com/"]
    {
        assert!(storage.bearer_token(resource).await.is_err());
    }
}
