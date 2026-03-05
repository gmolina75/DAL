namespace DATAMAP
{
    /// <summary>
    /// Represents a column definition for SQL script generation.
    /// </summary>
    public class ColumnDefinition
    {
        public string TableName { get; set; }
        public string ColumnName { get; set; }
        public string DataType { get; set; }
        public int? Length { get; set; }
        public int? Precision { get; set; }
        public int? Scale { get; set; }
        public bool IsNullable { get; set; }
    }
}