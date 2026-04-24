using CloudRedirect.Services;
using Xunit;

namespace CloudRedirect.Tests;

/// <summary>
/// Tests for OrphanBlobService.ComputeOrphans, the pure orphan-set
/// computation that drives the UI prune-orphan-blobs feature. Cloud I/O
/// paths (ScanAsync/PruneAsync) are integration-only and not covered here
/// because the project has no HTTP mocking infrastructure.
/// </summary>
public class OrphanBlobServiceTests
{
    private static IReadOnlySet<string> Referenced(params string[] names)
        => new HashSet<string>(names, StringComparer.Ordinal);

    [Fact]
    public void ComputeOrphans_NoCloudBlobs_ReturnsEmpty()
    {
        var orphans = OrphanBlobService.ComputeOrphans(Array.Empty<string>(), Referenced("a", "b"));
        Assert.Empty(orphans);
    }

    [Fact]
    public void ComputeOrphans_AllReferenced_ReturnsEmpty()
    {
        var cloud = new[] { "a.sav", "b.sav", "c.sav" };
        var refs = Referenced("a.sav", "b.sav", "c.sav");
        Assert.Empty(OrphanBlobService.ComputeOrphans(cloud, refs));
    }

    [Fact]
    public void ComputeOrphans_NoneReferenced_ReturnsAllSorted()
    {
        // Empty file_tokens.dat means every cloud blob is orphaned. This is
        // the worst-case prune scenario &#8212; if a user accidentally wipes
        // file_tokens.dat and then runs prune, they lose every cloud save
        // for that app. The UI layer's ListingComplete + confirmation flow
        // exists to prevent this.
        var cloud = new[] { "c.sav", "a.sav", "b.sav" };
        var orphans = OrphanBlobService.ComputeOrphans(cloud, Referenced());
        Assert.Equal(new[] { "a.sav", "b.sav", "c.sav" }, orphans);
    }

    [Fact]
    public void ComputeOrphans_MixedReferencedAndOrphan_ReturnsOnlyUnreferenced()
    {
        var cloud = new[] { "referenced.sav", "orphan1.sav", "referenced2.sav", "orphan2.sav" };
        var refs = Referenced("referenced.sav", "referenced2.sav");
        var orphans = OrphanBlobService.ComputeOrphans(cloud, refs);
        Assert.Equal(new[] { "orphan1.sav", "orphan2.sav" }, orphans);
    }

    [Fact]
    public void ComputeOrphans_CaseSensitive()
    {
        // A cloud blob named "Save.dat" is NOT satisfied by a file_tokens
        // entry of "save.dat" &#8212; the native side's std::unordered_map
        // keying is case-sensitive, so the UI must match to avoid declaring
        // a legitimately-referenced blob an orphan.
        var cloud = new[] { "Save.dat" };
        var refs = Referenced("save.dat");
        var orphans = OrphanBlobService.ComputeOrphans(cloud, refs);
        Assert.Single(orphans);
        Assert.Contains("Save.dat", orphans);
    }

    [Fact]
    public void ComputeOrphans_DuplicatesInCloud_Deduplicated()
    {
        var cloud = new[] { "orphan.sav", "orphan.sav", "orphan.sav" };
        var orphans = OrphanBlobService.ComputeOrphans(cloud, Referenced());
        Assert.Single(orphans);
        Assert.Equal("orphan.sav", orphans[0]);
    }

    [Fact]
    public void ComputeOrphans_EmptyAndNullFilenames_Skipped()
    {
        // Defensive: null/empty filenames in the cloud listing should never
        // reach this far but if they do we should not propagate them as
        // orphans (a Path.Combine with empty name would escape to the parent
        // directory on the folder provider).
        var cloud = new[] { "", null!, "real.sav" };
        var orphans = OrphanBlobService.ComputeOrphans(cloud, Referenced());
        Assert.Single(orphans);
        Assert.Equal("real.sav", orphans[0]);
    }

    [Fact]
    public void ComputeOrphans_DeterministicallySorted()
    {
        // The UI renders the orphan list in the confirm dialog; a
        // deterministic order helps users recognize the same app across
        // repeated scans.
        var cloud = new[] { "z", "m", "a", "b" };
        var orphans = OrphanBlobService.ComputeOrphans(cloud, Referenced());
        Assert.Equal(new[] { "a", "b", "m", "z" }, orphans);
    }

    [Fact]
    public void ComputeOrphans_LargeInput_PerformsLinearly()
    {
        // Sanity: 5000 cloud blobs, 4999 referenced. Should produce exactly 1
        // orphan. Documents the O(n) contract &#8212; if someone
        // accidentally introduces nested loops here, the regression shows up
        // as a timeout in CI.
        var cloud = new string[5000];
        for (int i = 0; i < 5000; i++) cloud[i] = $"blob_{i:D5}.sav";
        var refs = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < 5000; i++) if (i != 42) refs.Add($"blob_{i:D5}.sav");

        var orphans = OrphanBlobService.ComputeOrphans(cloud, refs);
        Assert.Single(orphans);
        Assert.Equal("blob_00042.sav", orphans[0]);
    }
}
