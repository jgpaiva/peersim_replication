package gsd.jgpaiva.utils;

import peersim.config.Configuration;

/***
 * object who's sole purpose is to read global configuration parameters
 * 
 * @author jgpaiva
 * 
 */
public class GlobalConfig {
	private static Integer idLength;
	private static Boolean detail;
	private static Integer replication;

	@SuppressWarnings("boxing")
	public static int getIdLength() {
		if (GlobalConfig.idLength == null) {
			GlobalConfig.idLength = Configuration.getInt("global" + '.' + "idLength");
		}
		return GlobalConfig.idLength;
	}

	@SuppressWarnings("boxing")
	public static boolean getDetail() {
		if (GlobalConfig.detail == null) {
			if (Configuration.contains("global" + '.' + "showDetail")) {
				GlobalConfig.detail = Configuration.getBoolean("global" + '.' + "showDetail");
			} else {
				GlobalConfig.detail = false;
			}
		}
		return GlobalConfig.detail;
	}

	public static int getReplication() {
		if (GlobalConfig.replication == null) {
			GlobalConfig.replication = Configuration.getInt("global" + '.' + "replication");
		}
		return GlobalConfig.replication;
	}
}
