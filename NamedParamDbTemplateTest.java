
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class NamedParamDbTemplateTest {

	@Test
	public void getWherePart() {
		Assert.assertEquals("k1= :k1 AND k2= :k2", NamedParamDbTemplate.getWherePart(ImmutableMap.of("k1", "v1", "k2", "v2")));
	}

	@Test
	public void getCreateValues() {
		Assert.assertEquals(":k1,:k2,:k3", NamedParamDbTemplate.getCreateValues(ImmutableList.of("k1", "k2", "k3"), ":", null));
	}

	@Test
	public void getCreateValuesWithSequenceNoId() {
		Assert.assertEquals(":k1,:k2,:k3", NamedParamDbTemplate.getCreateValues(ImmutableList.of("k1", "k2", "k3"), ":", "SEQUENCE.NEXTVAL"));
	}
	
	@Test
	public void getCreateValuesWithSequenceId() {
		Assert.assertEquals("SEQUENCE.NEXTVAL,:k2,:k3", NamedParamDbTemplate.getCreateValues(ImmutableList.of("ID", "k2", "k3"), ":", "SEQUENCE.NEXTVAL"));
	}
	
	@Test
	public void getUpdateKeyList() {
		Assert.assertEquals("k1= :k1,k2= :k2,k3= :k3", NamedParamDbTemplate.getUpdateKeyList(ImmutableList.of("k1", "k2", "k3")));
	}

	@Test
	public void getCreateSql() {
		Assert.assertEquals("INSERT INTO TABLE (k1,k2,k3,k4) VALUES (:k1,:k2,:k3,:k4)",
				NamedParamDbTemplate.getCreateSql("TABLE", ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"), null));
	}

	@Test
	public void getDeleteSql() {
		Assert.assertEquals("DELETE FROM TABLE WHERE k1= :k1 AND k2= :k2 AND k3= :k3 AND k4= :k4",
				NamedParamDbTemplate.getDeleteSql("TABLE", ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4")));
	}

	@Test
	public void getUpdateSql() {
		Assert.assertEquals("UPDATE TABLE SET k1= :k1,k2= :k2,k3= :k3,k4= :k4",
				NamedParamDbTemplate.getUpdateSql("TABLE", ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"), null));
	}

	@Test
	public void getUpdateWhereSql() {
		Assert.assertEquals("UPDATE TABLE SET k1= :k1,k2= :k2,k3= :k3,k4= :k4 WHERE k5= :k5 AND k6= :k6",
				NamedParamDbTemplate.getUpdateSql("TABLE", ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"), ImmutableMap.of("k5", "v5", "k6", "v6")));
	}

	@Test
	public void getQuerySqlNoColumn() {
		Assert.assertEquals("Select  *  FROM TABLE WHERE k1= :k1 AND k2= :k2 AND k3= :k3 AND k4= :k4",
				NamedParamDbTemplate.getQuerySql("TABLE", null, ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4")));
	}

	@Test
	public void getQuerySqlWithColumns() {
		Assert.assertEquals("Select k1,k3 FROM TABLE WHERE k1= :k1 AND k2= :k2 AND k3= :k3 AND k4= :k4",
				NamedParamDbTemplate.getQuerySql("TABLE", ImmutableList.of("k1", "k3"), ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4")));
	}

	@Test
	public void getQuerySqlWithColumnsNoWhere() {
		Assert.assertEquals("Select k1,k3 FROM TABLE", NamedParamDbTemplate.getQuerySql("TABLE", ImmutableList.of("k1", "k3"), null));
	}
