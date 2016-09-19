/**
 * 
 */
package iface;

/**
 * @author Administrator
 *
 */
public interface IDataPurgeService {
	
	// 验证Data Purge 规则服务类，返回：状态||错误信息
	// 如：SUCCESS||验证成功;    ERROR||验证规则SQL失败
	String validateDpRulesService(String sourceSysKey,
			                      String sourceTabOwner,
			                      String sourceTabName);
	
	// 启动数据清洗服务程序
	String startDpService(String sourceSysKey,
                          String sourceTabOwner,
                          String sourceTabName,
                          String dpStartDate,
                          String dpEndDate,
                          String initialDataImpFlag);
	
}
