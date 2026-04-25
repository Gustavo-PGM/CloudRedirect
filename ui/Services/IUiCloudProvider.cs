namespace CloudRedirect.Services;

/// <summary>
/// Per-provider implementation of the UI's cloud operations.
///
/// <para>The UI needs to talk to the same cloud backends as the DLL while
/// running in a separate process (the user typically launches us with Steam
/// closed). Rather than reimplement everything inline, each backend lives in
/// its own type behind this interface so <see cref="CloudProviderClient"/>
/// can stay a thin facade and the GDrive / OneDrive / folder logic can be
/// reasoned about independently.</para>
///
/// <para>Implementations are constructed by <see cref="UiCloudProviderFactory"/>
/// once per <see cref="CloudProviderClient"/> operation, holding any token-
/// path / sync-path bindings the operation needs. They share the client's
/// <c>HttpClient</c> and log sink rather than owning their own.</para>
///
/// <para>Result types live as nested records on <see cref="CloudProviderClient"/>
/// because <c>OrphanBlobService</c> references them by that path; moving them
/// would force unrelated caller churn.</para>
/// </summary>
internal interface IUiCloudProvider
{
    /// <summary>
    /// Recursively delete <c>CloudRedirect/{accountId}/{appId}/</c> on the
    /// provider. See <see cref="CloudProviderClient.DeleteAppDataAsync"/> for
    /// the dispatch entry point and full semantics.
    /// </summary>
    Task<CloudProviderClient.DeleteResult> DeleteAppDataAsync(
        string accountId, string appId, CancellationToken cancel);

    /// <summary>
    /// Enumerate direct file children of <c>CloudRedirect/{accountId}/{appId}/blobs/</c>.
    /// Honors the completeness contract documented on
    /// <see cref="CloudProviderClient.ListBlobsResult"/>: <c>Complete=false</c>
    /// on any pagination, auth, or transport failure.
    /// </summary>
    Task<CloudProviderClient.ListBlobsResult> ListAppBlobsAsync(
        string accountId, string appId, CancellationToken cancel);

    /// <summary>
    /// Delete the named blob filenames under
    /// <c>CloudRedirect/{accountId}/{appId}/blobs/</c>. Filenames are
    /// guaranteed safe by <see cref="CloudProviderClient"/> before dispatch
    /// (path separators, traversal, control chars, reserved device names,
    /// trailing dot/space already filtered out).
    /// </summary>
    Task<CloudProviderClient.DeleteBlobsResult> DeleteAppBlobsAsync(
        string accountId, string appId,
        IReadOnlyCollection<string> blobFilenames, CancellationToken cancel);
}
