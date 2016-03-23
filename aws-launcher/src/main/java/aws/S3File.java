package aws;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class S3File {


	private final S3Type s3Type;
	private final String bucket;
	private final String key;

	public S3File(String bucket, String key, S3Type s3Type) {
		this.bucket = bucket;
		this.key = key;
		this.s3Type = s3Type;
	}

	public S3File(String bucket, String key) {
		this(bucket,key,S3Type.BLOCK);
	}

	public String getBucket() {
		return bucket;
	}

	public String getKey() {
		return key;
	}

	public String toS3Path(){
		return  String.format("%s://%s/%s",s3Type.getPrefix(),bucket,key);
	}


	public static S3File fromS3Path(String s3Filename){
		String bucketName = null;
		S3Type type = S3Type.BLOCK;
		if (StringUtils.startsWith(s3Filename, S3Type.NATIVE.getPrefix()+"://")) {
			type = S3Type.NATIVE;
			bucketName = StringUtils.substringBetween(s3Filename, S3Type.NATIVE.getPrefix()+"://", "/");
		}

		if (StringUtils.startsWith(s3Filename,  S3Type.BLOCK.getPrefix()+"://")) {
			type = S3Type.BLOCK;
			bucketName = StringUtils.substringBetween(s3Filename, S3Type.BLOCK.getPrefix()+"://", "/");
		}

		String key = StringUtils.substringAfter(s3Filename, bucketName + "/");

		return new S3File(bucketName, key,type);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		S3File s3File = (S3File) o;

		return new EqualsBuilder().append(s3Type, s3File.s3Type).append(bucket,s3File.bucket).append(key,s3File.key).isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(s3Type).append(bucket).append(key).toHashCode();
	}
}
