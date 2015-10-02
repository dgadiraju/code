package filesystem;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class CopyMerge {

	public static void main(String[] args) throws Exception {
		String uri = args[0];
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path srcPath = new Path(args[0]);
		Path tgtPath = new Path(args[1]);
		
		boolean copyMerge = FileUtil.copyMerge(fs, srcPath, fs, tgtPath, false, conf, null);
		
		if(copyMerge)
			System.out.println("Merge Successful");
	}
	
}
