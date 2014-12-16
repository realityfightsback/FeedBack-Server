import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.FileUtils;

public class DB_Tester {

	public static void main(String[] args) throws Exception {
		DB_Tester db = new DB_Tester();
		db.registerVote("nowhere", 332, false);
	}

	public void getInfoOnURL(String URL, int userId) throws Exception {

		int UUID = getArticleUUID(URL);
		
		if(UUID == -1){
			//no info, add?
		}
		else{
			//Grab a bunch
			
		}
		
	}

	public void registerVote(String articleURL, int userId, boolean upVote)
			throws Exception {

		int UUID = getArticleUUID(articleURL);

		if (UUID != -1) {
			
			try (Connection conn2 = getConnection();
					PreparedStatement pStmnt2 = prepareVoteInsert(conn2,
							"INSERT INTO `votes` (`ArticleID`, `UserID`, `UpVote`, `DownVote`) "
									+ "VALUES (?, ?, ?, ?)", UUID, userId,
							upVote);) {
				pStmnt2.executeUpdate();
			}

		} else {
			addArticle(articleURL);
			registerVote(articleURL, userId, upVote);
		}
		// INSERT INTO `votes` (`ArticleID`, `UserID`, `UpVote`,
		// `DownVote`, `Date`) VALUES (1, 1, b'1', b'0', '2014-12-14
		// 11:45:15');

	}

	private int getArticleUUID(String articleURL) throws Exception {
		try (Connection conn = getConnection();
				PreparedStatement pStmnt = prepareUUIDSelect(conn,
						"SELECT uuid FROM articles WHERE url = ?", articleURL);
				ResultSet rs = pStmnt.executeQuery();) {
			if (rs.next()) {
				return rs.getInt(1);
			} else {
				return -1;
			}

		}

	}

	private PreparedStatement prepareVoteInsert(Connection conn, String sql,
			int articleId, int userId, boolean upVote) throws Exception {
		PreparedStatement pStmnt = conn.prepareStatement(sql);
		pStmnt.setInt(1, articleId);
		pStmnt.setInt(2, userId);
		if (upVote) {
			pStmnt.setBoolean(3, true);
			pStmnt.setBoolean(4, false);
		} else {
			pStmnt.setBoolean(3, false);
			pStmnt.setBoolean(4, true);

		}

		return pStmnt;
	}

	private PreparedStatement prepareUUIDSelect(Connection conn, String sql,
			String articleURL) throws Exception {
		PreparedStatement pStmnt = conn.prepareStatement(sql);
		pStmnt.setString(1, articleURL);
		return pStmnt;
	}

	private void addArticle(String articleURL) throws Exception {
		try (Connection conn = getConnection();
				PreparedStatement pStmnt = prepareUUIDSelect(conn,
						"INSERT INTO articles (URL) VALUES (?)", articleURL);) {
			pStmnt.executeUpdate();
		}

	}

	public void writeImportFile() throws Exception {

		File f = new File("C:\\Users\\Standard\\Desktop\\insert.sql");

		Timer timer = new Timer();

		StringBuilder sBuilder = new StringBuilder();

		FileUtils
				.writeStringToFile(
						f,
						"INSERT INTO `votes` (`ArticleID`, `UserID`, `UpVote`, `DownVote`, `Date`) VALUES\n");

		int numOfUsers = 500;
		int numOfArticles = 500;
		int writeAmount = 2000;

		for (int user = 0; user < numOfUsers; user++) {
			for (int article = 0; article < numOfArticles; article++) {
				sBuilder.append(String.format("(%d, %d, b'1', b'0', NULL),\n",
						article, user));
				// FileUtils.write(f,
				// String.format("(%d, %d, b'1', b'0', NULL)\n", article, user),
				// true);
			}
			// if (user % writeAmount == 0) {
			// if(user != 0){
			// System.out.println(writeAmount + " users down");
			// }
			FileUtils.write(f, sBuilder.toString(), true);
			sBuilder = new StringBuilder();
			// }

		}
		// FileUtils.write(f, sBuilder.toString(), true);

		System.out.println(timer.stop());

	}

	public void insertRows() throws Exception {
		Connection conn = getConnection();

		PreparedStatement stmnt = conn
				.prepareStatement("INSERT INTO `votes` (`ArticleID`, `UserID`, `UpVote`, `DownVote`) VALUES (?, ?, b'1', b'0')");

		int numOfUsers = 100000;
		int numOfArticles = 1000;

		for (int i = 0; i < numOfUsers; i++) {
			for (int j = 0; j < numOfArticles; j++) {
				stmnt.setInt(1, j);
				stmnt.setInt(2, i);
				stmnt.executeUpdate();

			}
			if (numOfUsers % 10000 == 0) {
				System.out.println("10k users down");
			}

		}

	}

	private Connection getConnection() throws Exception {
		Class.forName("com.mysql.jdbc.Driver").newInstance();

		Connection conn = DriverManager
				.getConnection("jdbc:mysql://localhost/test?"
						+ "user=root&password=root");
		return conn;
	}

	public void getArticlesTable(Statement stmnt) throws SQLException {
		ResultSet rs = stmnt.executeQuery("SELECT * FROM articles");

		while (rs.next()) {
			System.out.println(rs.getString("URL") + rs.getInt("UUID"));
		}
	}

	public void getVotesTable(Statement stmnt) throws SQLException {
		ResultSet rs = stmnt.executeQuery("SELECT * FROM votes");

		while (rs.next()) {
			System.out.println(rs.getInt("ArticleID") + " "
					+ rs.getInt("UserID") + rs.getInt("UpVote")
					+ rs.getInt("DownVote"));
		}

	}
}
