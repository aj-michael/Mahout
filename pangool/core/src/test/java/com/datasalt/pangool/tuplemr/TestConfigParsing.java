/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.tuplemr;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.thrift.test.A;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.utils.InstancesDistributor;

public class TestConfigParsing {

  private Schema schema1;
  private Schema schema2;
  private Schema schema3;

  @Before
  public void init() throws TupleMRException {
    this.schema1 = new Schema("schema1",
        Fields.parse("int_field:int, string_field:string,boolean_field:boolean"));
    this.schema2 = new Schema("schema2",
        Fields.parse("long_field:long,boolean_field:boolean, int_field:int"));

    List<Field> fields = new ArrayList<Field>();
    fields.add(Field.create("int_field", Type.INT));
    fields.add(Field.create("string_field", Type.STRING));
    fields.add(Field.create("long_field", Type.LONG));
    fields.add(Field.create("float_field", Type.FLOAT));
    fields.add(Field.create("double_field", Type.DOUBLE));
    fields.add(Field.create("boolean_field", Type.BOOLEAN));
    fields.add(Field.createEnum("enum_field", Order.class));
    fields.add(Field.createObject("thrift_field", A.class));
    this.schema3 = new Schema("schema3", fields);

  }

  @SuppressWarnings("serial")
  private static class DummyComparator implements RawComparator<Object>, Serializable {
    @Override
    public int compare(Object arg0, Object arg1) {
      return 0;
    }

    @Override
    public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof DummyComparator;
    }
  }

  @Test
  public void testSimple() throws TupleMRException, IOException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(schema1);
    b.setGroupByFields("int_field");
    TupleMRConfig conf = b.buildConf();
    TupleMRConfig deserConf = TupleMRConfig.parse(conf.toString());
    Assert.assertEquals(conf, deserConf);
    TupleMRConfig deserConf2 = TupleMRConfig.parse(deserConf.toString());
    Assert.assertEquals(conf, deserConf2);
  }

  @Test
  public void testExtended() throws TupleMRException, IOException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(schema1);
    b.addIntermediateSchema(schema2);
    b.addIntermediateSchema(schema3);
    b.setGroupByFields("int_field");
    b.setOrderBy(new OrderBy().add("int_field", Order.DESC).addSchemaOrder(Order.DESC)
        .add("boolean_field", Order.DESC));
    b.setRollupFrom("int_field");
    b.setSpecificOrderBy(schema3.getName(),
        new OrderBy().add("thrift_field", Order.ASC, Criteria.NullOrder.NULL_SMALLEST, new DummyComparator()));

    TupleMRConfig conf = b.buildConf();
    Configuration hconf = new Configuration();

    Set<String> instanceFiles = TupleMRConfig.set(conf, hconf);
    TupleMRConfig deserConf = TupleMRConfig.get(hconf);
    System.out.println(conf);
    System.out.println("------------");
    System.out.println(deserConf);

    Assert.assertEquals(conf, deserConf);
    hconf = new Configuration();
    instanceFiles.addAll(TupleMRConfig.set(deserConf, hconf));
    TupleMRConfig deserConf2 = TupleMRConfig.get(hconf);
    Assert.assertEquals(conf, deserConf2);
    
    for(String instanceFile: instanceFiles) {
    	InstancesDistributor.removeFromCache(hconf, instanceFile);
    }
  }

  @Test
  public void testWithCustomPartitionFields() throws TupleMRException, IOException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(schema1);
    b.addIntermediateSchema(schema2);
    b.addIntermediateSchema(schema3);
    b.setGroupByFields("int_field");
    b.setOrderBy(new OrderBy().add("int_field", Order.DESC).addSchemaOrder(Order.DESC)
        .add("boolean_field", Order.DESC));
    b.setRollupFrom("int_field");
    b.setSpecificOrderBy(schema3.getName(), new OrderBy().add("thrift_field", Order.ASC));
    b.setCustomPartitionFields("int_field", "boolean_field");

    TupleMRConfig conf = b.buildConf();
    TupleMRConfig deserConf = TupleMRConfig.parse(conf.toString());
    Assert.assertEquals(conf, deserConf);
    TupleMRConfig deserConf2 = TupleMRConfig.parse(deserConf.toString());
    Assert.assertEquals(conf, deserConf2);
    System.out.println(conf);
    System.out.println(deserConf2);
  }

  @Test
  public void testWithFieldAliases() throws TupleMRException, IOException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    Schema schema1 = new Schema("schema1", Fields.parse("user_id:int,operation:string,age:long,timestamp:int,country:string"));
    Schema schema2 = new Schema("schema2", Fields.parse("id:int,op:string,another_id:int,time:int"));

    b.addIntermediateSchema(schema1);
    b.addIntermediateSchema(schema2);
    b.setFieldAliases("schema1", new Aliases().add("id", "user_id").add("op", "operation"));
    b.setFieldAliases("schema2", new Aliases().add("timestamp", "time"));
    b.setGroupByFields("id", "op");
    b.setOrderBy(new OrderBy().add("op", Order.ASC).add("id", Order.DESC)
        .addSchemaOrder(Order.DESC).add("timestamp", Order.DESC));
    b.setSpecificOrderBy("schema1", new OrderBy().add("country", Order.DESC));

    TupleMRConfig conf = b.buildConf();
    Configuration hconf = new Configuration();

    TupleMRConfig.set(conf, hconf);
    TupleMRConfig deserConf = TupleMRConfig.get(hconf);
    System.out.println(conf);
    System.out.println("------------");
    System.out.println(deserConf);

    Assert.assertEquals(conf, deserConf);
    hconf = new Configuration();
    TupleMRConfig.set(deserConf, hconf);
    TupleMRConfig deserConf2 = TupleMRConfig.get(hconf);
    Assert.assertEquals(conf, deserConf2);
  }


}
