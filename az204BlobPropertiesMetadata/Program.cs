
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

//
const string STORAGE_ACCOUNT_CONNECTION_STRING = "[STORAGE-ACCOUNT-KEY]";
const string LOCAL_DIRECTORY = "./data/";

//
Console.WriteLine("Azure Storage Account # Blob Storage: exercise");

//

Console.WriteLine(">> Beginning");
ProcessAsync(STORAGE_ACCOUNT_CONNECTION_STRING).GetAwaiter().GetResult();
Console.WriteLine(">> End");

//
static async Task ProcessAsync(string storageAccountConnection)
{
    //
    var storageAccountServiceClient = await GetBlobServiceClientAsync(storageAccountConnection);

    //
    string blobContainerClientName = GetUniqueRandomNames();
    BlobContainerClient blobContainerClient = await GetBlobContainerClientAsync(storageAccountServiceClient, blobContainerClientName);

    //
    var blobLocalFilename = GetUniqueRandomNames();
    string blobFullPath = await CreateNewFileAsync(LOCAL_DIRECTORY, blobLocalFilename);

    //
    BlobClient blobClient = CreateNewBlobAsync(blobContainerClient, blobFullPath);

    //
    await UploadBlobAsync(blobFullPath, blobClient);

    //
    await ListBlobsAsync(blobContainerClient);

    //
    await GetBlobPropertiesAsync(blobContainerClient);

    //
    await GetContainerMetadataAsync(blobContainerClient);

    //
    await SetContainerMetadataAsync(blobContainerClient);

    //
    await GetBlobPropertiesAsync(blobContainerClient);

    //
    await GetContainerMetadataAsync(blobContainerClient);

    //
    string blobFullPathDownload = await DownloadBlobAsync(blobFullPath, blobClient);

    //
    await BlobContainerCleanupAsync(blobContainerClient, blobFullPath, blobClient, blobFullPathDownload);

}

static async Task SetContainerMetadataAsync(BlobContainerClient blobContainerClient)
{
    IDictionary<string,string> containerMetadata = new Dictionary<string,string>();

    containerMetadata.Add("key1", "a");
    containerMetadata.Add("key2", "b");

    await blobContainerClient.SetMetadataAsync(containerMetadata);
    Console.WriteLine("Azure Storage Account # Blob Storage: Container - Set Metadata");
    Console.ReadLine();

}

static async Task GetContainerMetadataAsync(BlobContainerClient blobContainerClient)
{

    var blobContainerProperties = await blobContainerClient.GetPropertiesAsync();

    foreach (var blobContainerMetadata in blobContainerProperties.Value.Metadata)    
    {
        Console.WriteLine("{0}, {1}", blobContainerMetadata.Key, blobContainerMetadata.Value);
    }
    Console.WriteLine("Azure Storage Account # Blob Storage: Container - Get Metadata");
    Console.ReadLine();
}

static async Task GetBlobPropertiesAsync(BlobContainerClient blobContainerClient)
{

    await foreach (var blob in blobContainerClient.GetBlobsAsync())
    {
        Console.WriteLine("{0}, {1}, {2}", blob.Name, blob.Properties.CreatedOn, blob.Properties.LastModified);
    }
    Console.WriteLine("Azure Storage Account # Blob Storage: Container - Get Properties");
    Console.ReadLine();
}

static async Task<BlobServiceClient> GetBlobServiceClientAsync(string storageAccountConnection)
{
    BlobServiceClient storageAccountServiceClient = new BlobServiceClient(storageAccountConnection);
    Console.WriteLine("Azure Storage Account # Blob Storage: Connected!");
    Console.ReadLine();

    return storageAccountServiceClient;
}

static async Task<BlobContainerClient> GetBlobContainerClientAsync(BlobServiceClient storageAccountServiceClient, string blobContainerClientName)
{
    BlobContainerClient blobContainerClient = await storageAccountServiceClient.CreateBlobContainerAsync(blobContainerClientName);
    Console.WriteLine("Azure Storage Account # Blob Storage: Blob Storage Container created!");
    Console.ReadLine();
    return blobContainerClient;
}

static string GetUniqueRandomNames()
{
    return ("ViCunha" + Guid.NewGuid()).ToString().ToLower().Replace("-", "");
}

static async Task<string> CreateNewFileAsync(string LOCAL_DIRECTORY, string blobLocalFilename)
{
    var blobFullPath = Path.Combine(LOCAL_DIRECTORY, blobLocalFilename);
    await File.AppendAllTextAsync(blobFullPath, "Hello Ana Paula!");

    Console.WriteLine("Azure Storage Account # Blob Storage: File created!");
    Console.ReadLine();
    return blobFullPath;
}

static BlobClient CreateNewBlobAsync(BlobContainerClient blobContainerClient, string blobFullPath)
{
    BlobClient blobClient = blobContainerClient.GetBlobClient(blobFullPath);
    Console.WriteLine("Azure Storage Account # Blob Storage: Blob created!");
    Console.ReadLine();
    return blobClient;
}

static async Task UploadBlobAsync(string blobFullPath, BlobClient blobClient)
{
    using (FileStream fileStream = File.OpenRead(blobFullPath))
    {
        await blobClient.UploadAsync(fileStream);
        fileStream.Close();
    }
    Console.WriteLine("Azure Storage Account # Blob Storage: Blob uploaded!");
    Console.ReadLine();
}

static async Task ListBlobsAsync(BlobContainerClient blobContainerClient)
{
    Console.WriteLine("Azure Storage Account # Blob Storage: Listing blob Beginning");

    await foreach (var blob in blobContainerClient.GetBlobsAsync())
    {
        Console.WriteLine("blob: {0}", blob.Name);
    }

    Console.WriteLine("Azure Storage Account # Blob Storage: Listing blob End");
    Console.ReadLine();
}

static async Task<string> DownloadBlobAsync(string blobFullPath, BlobClient blobClient)
{
    var blobFullPathDownload = blobFullPath.Replace(".txt", "DOWNLOADED.txt");
    BlobDownloadInfo blobDownloadInfo = await blobClient.DownloadAsync();

    using (FileStream fileStream = File.OpenWrite(blobFullPathDownload))
    {
        await blobDownloadInfo.Content.CopyToAsync(fileStream);
    }

    Console.WriteLine("Azure Storage Account # Blob Storage: Blob Downloaded");
    Console.ReadLine();
    return blobFullPathDownload;
}

static async Task BlobContainerCleanupAsync(BlobContainerClient blobContainerClient, string blobFullPath, BlobClient blobClient, string blobFullPathDownload)
{
    File.Delete(blobFullPathDownload);
    File.Delete(blobFullPath);

    await blobClient.DeleteAsync();
    await blobContainerClient.DeleteAsync();

    Console.WriteLine("Azure Storage Account # Blob Storage: Clean-Up Done!");
    Console.ReadLine();
}