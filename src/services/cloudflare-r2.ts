import fs from "fs"
import s3 from "aws-sdk/clients/s3";
import { AbstractFileService, DeleteFileType, FileServiceUploadResult, GetUploadedFileType, UploadStreamDescriptorType } from "@medusajs/medusa";
import stream from "stream";
import { EntityManager } from "typeorm";

interface Options {

  bucket: string,
  public_domain: string;
  access_key_id: string;
  secret_access_key: string;
  region: string;
  s3_endpoint: string;

}

export default class CloudflareR2Service extends AbstractFileService {

  protected manager_: EntityManager;
  protected transactionManager_: EntityManager;

  bucket_: string;
  public_domain_: string;
  accessKeyId_: string;
  secretAccessKey_: string;
  region_: string;
  s3Endpoint_: string;
  
  constructor({}, options: Options) {

    super({});

    const { bucket, public_domain, access_key_id, secret_access_key, region, s3_endpoint } = options;

    this.bucket_ = bucket;
    this.public_domain_ = public_domain;
    this.accessKeyId_ = access_key_id;
    this.secretAccessKey_ = secret_access_key;
    this.region_ = region;
    this.s3Endpoint_ = s3_endpoint;
    // this.awsConfigObject_ = options.aws_config_object

  }

  client() {

    return new s3({
      signatureVersion: "v4",
      region: "auto",
      endpoint: this.s3Endpoint_,
      accessKeyId: this.accessKeyId_,
      secretAccessKey: this.secretAccessKey_
    });

  }

  upload(fileData: Express.Multer.File) {

    return this.uploadFile(fileData);

  }

  uploadProtected(fileData: Express.Multer.File) {

    return this.uploadFile(fileData, { acl: "private" });

  }

  async uploadFile(fileData: Express.Multer.File, options?: { isProtected?: boolean; acl?: string }) {

    const client = this.client();

    console.log(fileData);

    const { path, originalname, mimetype: ContentType } = fileData;

    const params : s3.PutObjectRequest = {
      ACL: options?.acl ?? (options?.isProtected ? "private" : "public-read"),
      Bucket: this.bucket_,
      Body: fs.createReadStream(path),
      ContentType,
      Key: `${originalname}`
    };

    try {

      const { Key } = await client.upload(params).promise();

      const result: FileServiceUploadResult = {
        url: `${this.public_domain_}/${Key}`
      };

      return result;

    } catch (err) {
      console.error(err);
      throw new Error("An error occurred while uploading the file.");
    }

  }

  async delete(file: DeleteFileType) {

    const client = this.client();

    const params: s3.DeleteObjectRequest = {
      Bucket: this.bucket_,
      Key: `${file}`
    }

    await client.deleteObject(params).promise();
    
  }

  async getUploadStreamDescriptor(fileData: UploadStreamDescriptorType) {

    const pass = new stream.PassThrough()

    const fileKey = `${fileData.name}.${fileData.ext}`;

    const params: s3.PutObjectRequest = {
      ACL: fileData.acl ?? "private",
      Bucket: this.bucket_,
      Body: pass,
      Key: fileKey,
    };
    
    const client = this.client();

    return {
      writeStream: pass,
      promise: client.upload(params).promise(),
      url: `${this.public_domain_}/${fileKey}`,
      fileKey,
    };

  }

  async getDownloadStream(fileData: GetUploadedFileType) {
    
    const client = this.client();

    const params: s3.GetObjectRequest = {
      Bucket: this.bucket_,
      Key: `${fileData.fileKey}`,
    };

    return client.getObject(params).createReadStream();

  }

  async getPresignedDownloadUrl(fileData: GetUploadedFileType) {

    const client = this.client();

    const params = {
      Bucket: this.bucket_,
      Key: `${fileData.fileKey}`,
      Expires: 60,
    }

    return await client.getSignedUrlPromise("getObject", params)
  }

}