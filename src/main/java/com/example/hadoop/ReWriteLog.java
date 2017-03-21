package com.example.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;

public class ReWriteLog {
	
	public static void main(String[] args) throws Exception {
		String filePath = "/Users/jerry.li/data/xxxx/a2.log";
		File file = new File(filePath);
		if (!(file.exists() && file.isFile() && file.canRead())){
			return;
		}
		File newFile = new File("/Users/jerry.li/data/xxxx/newlog.log");
		if(newFile.exists()){
			newFile.delete();
		}
		newFile.createNewFile();
		FileOutputStream out = new FileOutputStream(newFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String data = null;
		while((data = br.readLine())!=null)
		{
			StringBuilder sb = new StringBuilder();
			sb.append(data.substring(0, data.indexOf(" ")));
			sb.append("\t");
			data = data.substring(data.indexOf("[")+1);
			sb.append(data.substring(0, data.indexOf("]")));
			sb.append("\t");
			data = data.substring(data.indexOf("\"")+1);
			sb.append(data.substring(0, data.indexOf("\"")));
			sb.append("\t");
			data = data.substring(data.indexOf("\" ")+2);
			sb.append(data.substring(0, data.indexOf(" ")));
			sb.append("\t");
			data = data.substring(data.indexOf(" ")+1);
			sb.append(data.substring(0, data.indexOf(" ")));
			sb.append("\t");
			data = data.substring(0, data.length() - 1);
			data = data.substring(data.lastIndexOf("\"") + 1);
			if (data.indexOf(", ") > -1){
				data = data.substring(data.indexOf(", ")+2);
			}
			sb.append(data);
			sb.append("\n");
			byte[] bt = new byte[1024];
			bt = sb.toString().getBytes();
			out.write(bt);
		}
		out.close();
	}
	
}
