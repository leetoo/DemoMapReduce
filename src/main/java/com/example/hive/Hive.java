package com.example.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hive.*;

public class Hive {
	
	public static void main(String[] args) {
		Connection conn = null;
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/hive");
			Statement st = conn.createStatement();
			String sql = "select count(1) from nginx_logs";
			ResultSet result = st.executeQuery(sql);
			System.out.println(sql);
			while(result.next()){
				System.out.println(result.getString(1));
			}
			sql = "select * from t_nlogs";
			result = st.executeQuery(sql);
			while(result.next()){
				System.out.print(result.getInt(1));
				System.out.print("\t");
				System.out.println(result.getString(2));
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			
		} finally {
			if (conn != null){
				try {
					conn.close();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	
}
