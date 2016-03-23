package aws;

public enum S3Type {
		NATIVE("s3n"),
		BLOCK("s3");

		private final String prefix;

		S3Type(String prefix){
			this.prefix = prefix;
		}


		public String getPrefix(){
			return prefix;
		}
	}