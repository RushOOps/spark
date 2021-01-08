package util;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 字符串工具类
 * @author crash
 *
 */
public class StringUtil {

	/**
	 * 判断字符串是否为空
	 * @param str
	 * @return
	 */
	public static boolean isEmpty(String str) {
		return "".equals(str) || str == null;
	}

	/**
	 * 判断字符串是否不为空
	 * @param str
	 * @return
	 */
	public static boolean isNotEmpty(String str) {
		return !"".equals(str) && str != null;
	}

	/**
	 * 判断字符串数组里是否存在某字符串
	 * @param str
	 * @param strArr
	 * @return
	 */
	public static boolean existStrArr(String str, String[] strArr) {
		for (String s : strArr) {
			if (s.equals(str)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 过滤字符串中的html标签
	 * @param inputString
	 * @return
	 */
	public static String Html2Text(String inputString) {
		// 过滤html标签
		String htmlStr = inputString; // 含html标签的字符串
		String textStr = "";
		Pattern p_script;
		java.util.regex.Matcher m_script;
		Pattern p_style;
		java.util.regex.Matcher m_style;
		Pattern p_html;
		java.util.regex.Matcher m_html;
		Pattern p_cont1;
		java.util.regex.Matcher m_cont1;
		Pattern p_cont2;
		java.util.regex.Matcher m_cont2;
		try {
			String regEx_script = "<[\\s]*?script[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?script[\\s]*?>"; // 定义script的正则表达式{或<script[^>]*?>[\\s\\S]*?<\\/script>
			// }
			String regEx_style = "<[\\s]*?style[^>]*?>[\\s\\S]*?<[\\s]*?\\/[\\s]*?style[\\s]*?>"; // ����style��������ʽ{��<style[^>]*?>[\\s\\S]*?<\\/style>
			// }
			String regEx_html = "<[^>]+>"; // 定义HTML标签的正则表达式
			String regEx_cont1 = "[\\d+\\s*`~!@#$%^&*\\(?~！@#¥%⋯⋯&*（）——+|{}【】‘：”“’_]";  // 定义HTML标签的正则表达式
			String regEx_cont2 = "[\\w[^\\W]*]";  // 定义HTML标签的正则表达式[a-zA-Z]
			p_script = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE);
			m_script = p_script.matcher(htmlStr);
			htmlStr = m_script.replaceAll(""); // 过滤script标签
			p_style = Pattern.compile(regEx_style, Pattern.CASE_INSENSITIVE);
			m_style = p_style.matcher(htmlStr);
			htmlStr = m_style.replaceAll(""); // 过滤style标签
			p_html = Pattern.compile(regEx_html, Pattern.CASE_INSENSITIVE);
			m_html = p_html.matcher(htmlStr);
			htmlStr = m_html.replaceAll(""); // 过滤html标签
			p_cont1 = Pattern.compile(regEx_cont1, Pattern.CASE_INSENSITIVE);
			m_cont1 = p_cont1.matcher(htmlStr);
			htmlStr = m_cont1.replaceAll(""); // 过滤其他标签
			p_cont2 = Pattern.compile(regEx_cont2, Pattern.CASE_INSENSITIVE);
			m_cont2 = p_cont2.matcher(htmlStr);
			htmlStr = m_cont2.replaceAll(""); // 过滤html标签
			textStr = htmlStr;
		} catch (Exception e) {
			System.err.println("Html2Text: " + e.getMessage());
		}
		return textStr;// 返回文本字符串
	}
	
	/**
	 * 格式化模糊查询
	 * @param str
	 * @return
	 */
	public static String formatLike(String str){
		if(isNotEmpty(str)){
			return "%"+str+"%";
		}else{
			return null;
		}
	}

	public static String arabicNumeralToChinese(String str){
		Map<String, String> map = new HashMap<String, String>(){
			{
				put("0", "零");
				put("1", "一");
				put("2", "二");
				put("3", "三");
				put("4", "四");
				put("5", "五");
				put("6", "六");
				put("7", "七");
				put("8", "八");
				put("9", "九");
			}
		};

		for (String s : map.keySet()){
			if(str.contains(s)){
				str = str.replace(s, map.get(s));
			}
		}

		return str;
	}

	public static boolean strEquals(String str1, String str2){
		if(str1 == null || str2 == null) return false;
		return str1.equals(str2);
	}
}
