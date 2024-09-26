using Microsoft.Data.Sqlite;

class Program
{
    private static byte[] GetBytes(SqliteDataReader reader, int index)
    {
        byte[] bytes;

        using var stream = reader.GetStream(index);
        using var binaryReader = new BinaryReader(stream);
        bytes = binaryReader.ReadBytes((int)stream.Length);
        
        return bytes;
    }

    public static void Execute(SqliteConnection connection, string sql, params (string, object)[] parameters)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;

        foreach ((string key, object value) in parameters)
        {
            if (value == null)
            {
                command.Parameters.AddWithValue(key, DBNull.Value);
            }
            else
            {
                command.Parameters.AddWithValue(key, value);
            }
        }
        command.ExecuteNonQuery();   
    }



    static void Main(string[] args)
    {

        var factData = new Byte[] { 1 };
        var fileData1 = new Byte[] { 2 };
        var fileData2 = new Byte[] { 3 };

        var connection = new SqliteConnection($"Data Source=:memory:");

        connection.Open();

        // These are stripped down copies of our application tables.
        // We have a many-to-many relation between CaseFileModel and CaseFactModel,
        // so we use CaseFactModel_CaseFileModel to connect the tables.

        Execute(connection, @"CREATE TABLE CaseFactModel (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
                        ImageData BLOB
                        )");

        Execute(connection, @"CREATE TABLE CaseFileModel (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
                        ThumbnailData BLOB
                        )");

        Execute(connection, @"CREATE TABLE CaseFactModel_CaseFileModel (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, 
                        CaseFactModelId INTEGER,
                        CaseFileModelId INTEGER,
                        FOREIGN KEY (CaseFactModelId) REFERENCES CaseFactModel (Id) ON DELETE CASCADE,
                        FOREIGN KEY (CaseFileModelId) REFERENCES CaseFileModel (Id) ON DELETE CASCADE
                        )");



        // Creating one CaseFactModel and two CaseFileModels and linking them together
        // We insert dummy binary data to illustrate the issue where the CaseFactModel will get ImageDate of one byte = 1,
        // the first CaseFileModel have ThumbnailData = 2, and the second have ThumbnailData = 3

        Execute(connection, @"INSERT INTO CaseFactModel (ImageData) VALUES ($factData)", ("factData", factData));
        Execute(connection, @"INSERT INTO CaseFileModel (ThumbnailData) VALUES ($fileData1), ($fileData2)", ("fileData1", fileData1), ("fileData2", fileData2));
        Execute(connection, @"INSERT INTO CaseFactModel_CaseFileModel (CaseFactModelId, CaseFileModelId) VALUES (1,1)");
        Execute(connection, @"INSERT INTO CaseFactModel_CaseFileModel (CaseFactModelId, CaseFileModelId) VALUES (1,2)");


        // With the following select we expect that the first row should have ThumbnailData = 2 and the second should have ThumbnailData = 3,
        // but both rows have ThumbnailData = 2
        // If we run the same tests using DB Browser for SQLite (https://sqlitebrowser.org/) where we get the expected result.

        Console.WriteLine($"including fact.Id in the select statement gives wrong blob data in second row");

        var command = connection.CreateCommand();
        command.CommandText = "SELECT fact.Id, fact.ImageData, file.ThumbnailData FROM CaseFactModel_CaseFileModel link LEFT JOIN CaseFactModel fact on link.CaseFactModelId = fact.Id LEFT JOIN CaseFileModel file on link.CaseFileModelId = file.id";
       
        var reader = command.ExecuteReader();

        if (reader != null)
        {
            while (reader.Read())
            {
                var linkFactImageData = GetBytes(reader, 1);
                var linkFileThumbnailData = GetBytes(reader, 2);

                Console.WriteLine($"ImageData[0] = {linkFactImageData[0]}, ThumbnailData[0] = {linkFileThumbnailData[0]}");
            }
        }


        // If we run the same select as above, but removed the "fact.Id" part we get the expected result
        // where first row have ThumbnailData = 2 and the second should have ThumbnailData = 3.

        Console.WriteLine($"excluding fact.Id from the select statement gives correct blob data in second row");

        var command2 = connection.CreateCommand();
        command2.CommandText = "SELECT fact.ImageData, file.ThumbnailData FROM CaseFactModel_CaseFileModel link LEFT JOIN CaseFactModel fact on link.CaseFactModelId = fact.Id LEFT JOIN CaseFileModel file on link.CaseFileModelId = file.id";
       
        var reader2 = command2.ExecuteReader();

        if (reader2 != null)
        {
            while (reader2.Read())
            {
                var linkFactImageData = GetBytes(reader2, 0);
                var linkFileThumbnailData = GetBytes(reader2, 1);

                Console.WriteLine($"ImageData[0] = {linkFactImageData[0]}, ThumbnailData[0] = {linkFileThumbnailData[0]}");
            }
        }

        return;
    }
}
