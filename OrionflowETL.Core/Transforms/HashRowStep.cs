using System.Security.Cryptography;
using System.Text;
using OrionflowETL.Core.Abstractions;
using OrionflowETL.Core.Models;

namespace OrionflowETL.Core.Transforms;

/// <summary>
/// Computes a hash (SHA256 by default, or MD5) of one or more column values
/// and writes the result as a lowercase hexadecimal string into a new column.
///
/// Typical use: Hash Diff Load strategy — compute a row fingerprint to detect
/// whether a row has changed since the last load, avoiding unnecessary updates.
///
/// Example:
///   new HashRowStep(
///       targetColumn: "__row_hash",
///       columns: ["CustomerName", "Address", "Phone"],
///       algorithm: HashAlgorithmType.SHA256)
///
/// The output column value is a stable, lowercase hex string such as:
///   "3b4c2a1d9ef..."
///
/// Column values are concatenated using a pipe separator before hashing to
/// reduce hash collisions from adjacent columns (e.g. "AB" + "C" ≠ "A" + "BC").
/// Null values are represented as the literal string "∅" to distinguish them
/// from empty strings.
/// </summary>
public sealed class HashRowStep : IPipelineStep
{
    private readonly string   _targetColumn;
    private readonly string[] _columns;
    private readonly HashAlgorithmType _algorithm;

    /// <summary>
    /// Initializes a new <see cref="HashRowStep"/>.
    /// </summary>
    /// <param name="targetColumn">
    ///   Name of the new column that will receive the hash value.
    ///   If a column with this name already exists, it will be overwritten.
    /// </param>
    /// <param name="columns">
    ///   Ordered list of columns to include in the hash.
    ///   If empty, ALL columns in the row are hashed (in their natural order).
    /// </param>
    /// <param name="algorithm">Hashing algorithm. Default: SHA256.</param>
    public HashRowStep(
        string targetColumn,
        string[]? columns = null,
        HashAlgorithmType algorithm = HashAlgorithmType.SHA256)
    {
        if (string.IsNullOrWhiteSpace(targetColumn))
            throw new ArgumentException("targetColumn cannot be null or empty.", nameof(targetColumn));

        _targetColumn = targetColumn;
        _columns      = columns ?? [];
        _algorithm    = algorithm;
    }

    public string Name => $"HashRow[{_algorithm}→{_targetColumn}]";

    /// <inheritdoc/>
    public IRow Execute(IRow row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        var columnsToHash = _columns.Length > 0 ? _columns : [.. row.Columns];
        var hashInput     = BuildHashInput(row, columnsToHash);
        var hashValue     = ComputeHash(hashInput, _algorithm);

        // Copy all existing columns + add/overwrite the hash column
        var data = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var col in row.Columns)
            data[col] = row.Get<object?>(col);

        data[_targetColumn] = hashValue;

        return new Row(data);
    }

    // ── Helpers ─────────────────────────────────────────────────────

    private static string BuildHashInput(IRow row, string[] columns)
    {
        var sb = new StringBuilder();
        foreach (var col in columns)
        {
            if (sb.Length > 0) sb.Append('|');

            var value = row.Columns.Contains(col) ? row.Get<object?>(col) : null;
            sb.Append(value is null ? "∅" : value.ToString());
        }
        return sb.ToString();
    }

    private static string ComputeHash(string input, HashAlgorithmType algorithm)
    {
        var bytes = Encoding.UTF8.GetBytes(input);

        byte[] hash = algorithm switch
        {
            HashAlgorithmType.MD5    => MD5.HashData(bytes),
            HashAlgorithmType.SHA256 => SHA256.HashData(bytes),
            _                        => SHA256.HashData(bytes)
        };

        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}

/// <summary>
/// Supported hashing algorithms for <see cref="HashRowStep"/>.
/// </summary>
public enum HashAlgorithmType
{
    SHA256,
    MD5
}
