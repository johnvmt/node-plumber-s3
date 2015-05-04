// using S3 client https://github.com/andrewrk/node-s3-client

module.exports = function(plumber, config) {
	return new S3FileStorage(plumber, config);
};

var fs = require('fs');
var s3 = require('s3');

function S3FileStorage(plumber, config) {
	this.bucket = config.bucket;
	this.s3Client = s3.createClient({
		s3Options: config.aws
	});
}

S3FileStorage.prototype.putFile = function(localFilename, remoteFilename, callback) {
	var self = this;
	var params = {
		localFile: localFilename,
		s3Params: {
			Bucket: self.bucket,
			Key: remoteFilename
		}
	};
	var uploader = self.s3Client.uploadFile(params);
	uploader.on('error', function(error) {
		callback(error, null);
	});
	uploader.on('progress', function() {
		console.log("progress", uploader.progressMd5Amount,
			uploader.progressAmount, uploader.progressTotal);
	});
	uploader.on('end', function() {
		callback(null, true);
	});
};

S3FileStorage.prototype.getFile = function(remoteFilename, localFilename, callback) {
	var self = this;
	var returned = false;
	var params = {
		localFile: localFilename,
		s3Params: {
			Bucket: self.bucket,
			Key: remoteFilename
		}
	};
	var downloader = self.s3Client.downloadFile(params);
	downloader.on('error', function(error) {
		if(!returned) {
			returned = true;
			callback(error, null);
		}
	});
	downloader.on('progress', function() {
		if(downloader.progressAmount == downloader.progressTotal && !returned) {
			returned = true;
			callback(null, true);
		}
	});
	downloader.on('end', function() {
		if(!returned) {
			returned = true;
			callback(null, true);
		}
	});
};

S3FileStorage.prototype.putDir = function(localDir, remoteDir, callback) {
	var self = this;
	var params = {
		localDir: localDir,
		deleteRemoved: true,
		s3Params: {
			Bucket: self.bucket,
			Prefix: remoteDir
		}
	};
	var uploader = self.s3Client.uploadDir(params);
	uploader.on('error', function(error) {
		callback(error, null);
	});
	uploader.on('progress', function() {
		console.log("progress", uploader.progressAmount, uploader.progressTotal);
	});
	uploader.on('end', function() {
		callback(null, true);
	});
};

S3FileStorage.prototype.getDir = function(remoteDir, localDir, callback) {
	var self = this;
	var params = {
		localDir: localDir,
		deleteRemoved: true,
		s3Params: {
			Bucket: self.bucket,
			Prefix: remoteDir
		}
	};
	var downloader = self.s3Client.downloadDir(params);
	downloader.on('error', function(error) {
		callback(error, null);
	});
	downloader.on('progress', function() {
		console.log("progress", downloader.progressAmount, downloader.progressTotal);
	});
	downloader.on('end', function() {
		callback(null, true);
		console.log("done downloading");
	});
};
