package oracle.demo.oow.bd.util.hbase.loader;

import oracle.demo.oow.bd.dao.hbase.UserDao;
import oracle.demo.oow.bd.to.CustomerTO;

public class UserTest {

	public static void main(String[] args) {
		UserDao userDao = new UserDao();
		
		CustomerTO customerTO = userDao.getCustomerByCredential("guest10", "welcome1");
		System.out.println(customerTO.getUserName()+","+customerTO.getId());
	}
}
