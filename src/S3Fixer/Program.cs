using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using FluentDateTime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace S3Fixer
{
    /// <summary>
    /// Ideas and Enhancements
    ///     - Config the Keys 
    ///     - Config the DateTime target modified date
    ///     - Introduce option for moving old objects to Glacier
    ///     - INtroduce Cron Schedules and Hangfire
    /// </summary>
    class Program
    {
        private const string bucketName = "*** bucket ****";
        private const string keyName = "*** object key ***";
        private const string AccessKey = "*** access key ***";
        private const string SecretKey = "*** secret key ***";
        private static IAmazonS3 client;
        private static readonly DateTime _modifiedDate =  new DateTime(2019, 4, 16);

        static void Main(string[] args)
        {            
            client = new AmazonS3Client(AccessKey, SecretKey, RegionEndpoint.EUWest1);

            Console.WriteLine("Grabbing objects from S3...");

            List<S3Object> s3Objects = ListObjects(_modifiedDate).Result;

            Console.WriteLine("Objects received from S3: {0}", s3Objects?.Count);

            Console.WriteLine("S3 Operations complete.");

            Console.ReadKey();
        }

        /// <summary>
        /// This routine will retireve all objects within the specified S3 Bucket
        /// Any objects which have a modified date older than our target will be removed 
        /// </summary>
        /// <param name="modifiedDate"></param>
        /// <returns></returns>
        private static async Task<List<S3Object>> ListObjects(DateTime modifiedDate)
        {
            try
            {
                List<S3Object> s3Objects = new List<S3Object>();

                ListObjectsV2Request request = new ListObjectsV2Request()
                {
                    BucketName = bucketName
                };

                ListObjectsV2Response response;

                Console.WriteLine("Connecting to S3...");

                do
                {
                    response = await client.ListObjectsV2Async(request);

                    foreach (S3Object entry in response.S3Objects)
                    {
                        if(entry.LastModified.IsAfter(modifiedDate))
                        {
                            var listResponse = client.ListVersionsAsync(new ListVersionsRequest
                            {
                                BucketName = bucketName,
                                Prefix = entry.Key
                            });

                            S3ObjectVersion deleteMarkerVersion = listResponse.Result.Versions.FirstOrDefault(x => x.IsLatest);

                            if (deleteMarkerVersion != null)
                            {
                                await client.DeleteObjectAsync(new DeleteObjectRequest
                                {
                                    BucketName = bucketName,
                                    Key = entry.Key,
                                    VersionId = deleteMarkerVersion.VersionId
                                });
                            }

                            s3Objects.Add(entry);
                        }
                    }

                    Console.WriteLine("Next Continuation Token: {0}", response.NextContinuationToken);

                    request.ContinuationToken = response.NextContinuationToken;

                } while (response.IsTruncated);

                return s3Objects;
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);

                return null;
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);

                return null;
            }
        }
    }
}
