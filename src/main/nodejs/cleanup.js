import { S3Client, DeleteObjectCommand } from '@aws-sdk/client-s3';

const s3Client = new S3Client();

export const cleanupBatchHandler = async (event) => {
  console.log('Input event:', JSON.stringify(event));
  const { mapJobsInputBucket, inputJobsFile } = event;
  console.log(`Delete s3://${mapJobsInputBucket}:${inputJobsFile}`);
  const res = await s3Client.send(new DeleteObjectCommand({
    Bucket: mapJobsInputBucket,
    Key: inputJobsFile,
  }));
  console.log(`Delete s3://${mapJobsInputBucket}:${inputJobsFile} result:`, res);
  return event;
};
