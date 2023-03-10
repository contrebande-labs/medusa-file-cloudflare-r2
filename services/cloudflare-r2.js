var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);
var cloudflare_r2_exports = {};
__export(cloudflare_r2_exports, {
  default: () => CloudflareR2Service
});
module.exports = __toCommonJS(cloudflare_r2_exports);
var import_fs = __toESM(require("fs"));
var import_s3 = __toESM(require("aws-sdk/clients/s3"));
var import_medusa = require("@medusajs/medusa");
var import_stream = __toESM(require("stream"));
class CloudflareR2Service extends import_medusa.AbstractFileService {
  manager_;
  transactionManager_;
  bucket_;
  prefix_;
  public_url_;
  accessKeyId_;
  secretAccessKey_;
  s3Endpoint_;
  constructor({}, options) {
    super({});
    const { bucket, prefix = "", public_url, access_key_id, secret_access_key, s3_endpoint } = options;
    this.bucket_ = bucket;
    this.prefix_ = prefix;
    this.public_url_ = public_url;
    this.accessKeyId_ = access_key_id;
    this.secretAccessKey_ = secret_access_key;
    this.s3Endpoint_ = s3_endpoint;
  }
  client() {
    return new import_s3.default({
      signatureVersion: "v4",
      region: "auto",
      endpoint: this.s3Endpoint_,
      accessKeyId: this.accessKeyId_,
      secretAccessKey: this.secretAccessKey_
    });
  }
  upload(fileData) {
    return this.uploadFile(fileData);
  }
  uploadProtected(fileData) {
    return this.uploadFile(fileData, { acl: "private" });
  }
  async uploadFile(fileData, options) {
    const client = this.client();
    const { path, originalname, mimetype: ContentType } = fileData;
    const Key = this.getFileKey(originalname);
    const params = {
      ACL: options?.acl ?? (options?.isProtected ? "private" : "public-read"),
      Bucket: this.bucket_,
      Body: import_fs.default.createReadStream(path),
      ContentType,
      Key
    };
    try {
      const { Key: returnedKey } = await client.upload(params).promise();
      const result = {
        url: `${this.public_url_}/${returnedKey}`
      };
      return result;
    } catch (err) {
      console.error(err);
      throw new Error("An error occurred while uploading the file.");
    }
  }
  async delete(file) {
    const client = this.client();
    const params = {
      Bucket: this.bucket_,
      Key: `${file}`
    };
    await client.deleteObject(params).promise();
  }
  async getUploadStreamDescriptor(fileData) {
    const pass = new import_stream.default.PassThrough();
    const fileKey = this.getFileKey(`${fileData.name}.${fileData.ext}`);
    const params = {
      ACL: fileData.acl ?? "private",
      Bucket: this.bucket_,
      Body: pass,
      Key: fileKey
    };
    const client = this.client();
    return {
      writeStream: pass,
      promise: client.upload(params).promise(),
      url: `${this.public_url_}/${fileKey}`,
      fileKey
    };
  }
  async getDownloadStream(fileData) {
    const client = this.client();
    const params = {
      Bucket: this.bucket_,
      Key: `${fileData.fileKey}`
    };
    return client.getObject(params).createReadStream();
  }
  async getPresignedDownloadUrl(fileData) {
    const client = this.client();
    const params = {
      Bucket: this.bucket_,
      Key: `${fileData.fileKey}`,
      Expires: 60
    };
    return await client.getSignedUrlPromise("getObject", params);
  }
  getFileKey(fileName) {
    const prefixPath = this.prefix_.trim().length > 0 ? `${this.prefix_}/` : "";
    return `${prefixPath}${fileName}`;
  }
}
