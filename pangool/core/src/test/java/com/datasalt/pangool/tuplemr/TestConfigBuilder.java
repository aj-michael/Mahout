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

import com.datasalt.pangool.BaseTest;
import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.thrift.test.A;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.serialization.AvroFieldSerialization;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Drum of tests tochísima ( Batería de tests)
 */
public class TestConfigBuilder extends BaseTest {

  @Test
  public void testCorrectMinimal() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.setGroupByFields("a");
    TupleMRConfig conf = b.buildConf();
    conf.getSerializationInfo();
  }

  @Test
  public void testCorrect2() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("a:int,b:string")));
    b.setGroupByFields("a");
    b.setOrderBy(new OrderBy().add("a", Order.ASC).addSchemaOrder(Order.DESC)
        .add("b", Order.DESC));
    TupleMRConfig conf = b.buildConf();
    conf.getSerializationInfo();
  }

  @Test
  public void testCommonSortByToCriteria() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields
        .parse("a:int,b:string,c:string,blabla:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields
        .parse("a:int,c:string,b:string,bloblo:string")));
    b.setGroupByFields("c", "b");
    b.setOrderBy(new OrderBy().add("b", Order.ASC).add("c", Order.DESC)
        .addSchemaOrder(Order.DESC).add("a", Order.DESC));
    b.setSpecificOrderBy("schema1", new OrderBy().add("blabla", Order.DESC));
    TupleMRConfig config = b.buildConf();
    config.getSerializationInfo();
    {
      List<SortElement> expectedCommon = new ArrayList<SortElement>();
      expectedCommon.add(new SortElement("b", Order.ASC, Criteria.NullOrder.NULL_SMALLEST));
      expectedCommon.add(new SortElement("c", Order.DESC, Criteria.NullOrder.NULL_SMALLEST));
      Assert.assertEquals(new Criteria(expectedCommon), config.getCommonCriteria());
    }
    {
      List<SortElement> expectedSchema1 = new ArrayList<SortElement>();
      expectedSchema1.add(new SortElement("a", Order.DESC, Criteria.NullOrder.NULL_SMALLEST));
      expectedSchema1.add(new SortElement("blabla", Order.DESC, Criteria.NullOrder.NULL_SMALLEST));
      Assert.assertEquals(new Criteria(expectedSchema1), config.getSpecificOrderBys()
          .get(0));
    }
    {
      List<SortElement> expectedSchema2 = new ArrayList<SortElement>();
      expectedSchema2.add(new SortElement("a", Order.DESC, Criteria.NullOrder.NULL_SMALLEST));
      Assert.assertEquals(new Criteria(expectedSchema2), config.getSpecificOrderBys()
          .get(1));
    }

  }

  @Test
  public void testAliases1() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields
        .parse("ax:int,bx:string,cx:string,blablax:string,p2:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields
        .parse("ay:int,cy:string,by:string,blobloy:string,p:string")));
    {
      Aliases aliases1 = new Aliases();
      aliases1.add("a", "ax");
      aliases1.add("b", "bx");
      aliases1.add("c", "cx");
      aliases1.add("blabla", "blablax");
      aliases1.add("p", "p2");
      b.setFieldAliases("schema1", aliases1);
    }
    {
      Aliases aliases2 = new Aliases();
      aliases2.add("a", "ay");
      aliases2.add("b", "by");
      aliases2.add("c", "cy");
      aliases2.add("bloblo", "blobloy");
      b.setFieldAliases("schema2", aliases2);
    }

    b.setGroupByFields("c", "b");
    b.setOrderBy(new OrderBy().add("b", Order.ASC).add("c", Order.DESC)
        .addSchemaOrder(Order.DESC).add("a", Order.DESC));
    b.setSpecificOrderBy("schema1", new OrderBy().add("blabla", Order.DESC));
    b.setCustomPartitionFields("p");
    TupleMRConfig config = b.buildConf();
    SerializationInfo serInfo = config.getSerializationInfo();
    System.out.println(serInfo.getCommonSchema());
    System.out.println(serInfo.getPartitionFieldsIndexes());

    {
      List<SortElement> expectedCommon = new ArrayList<SortElement>();
      expectedCommon.add(new SortElement("b", Order.ASC, Criteria.NullOrder.NULL_SMALLEST));
      expectedCommon.add(new SortElement("c", Order.DESC, Criteria.NullOrder.NULL_SMALLEST));
      Assert.assertEquals(new Criteria(expectedCommon), config.getCommonCriteria());
    }
    {
      List<SortElement> expectedSchema1 = new ArrayList<SortElement>();
      expectedSchema1.add(new SortElement("a", Order.DESC, Criteria.NullOrder.NULL_SMALLEST));
      expectedSchema1.add(new SortElement("blabla", Order.DESC, Criteria.NullOrder.NULL_SMALLEST));
      Assert.assertEquals(new Criteria(expectedSchema1), config.getSpecificOrderBys()
          .get(0));
    }
    {
      List<SortElement> expectedSchema2 = new ArrayList<SortElement>();
      expectedSchema2.add(new SortElement("a", Order.DESC, Criteria.NullOrder.NULL_SMALLEST));
      Assert.assertEquals(new Criteria(expectedSchema2), config.getSpecificOrderBys()
          .get(1));
    }

  }


  @Test
  public void testCommonOrderGeneratedImplicitlyFromGroupFields() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields
        .parse("a:int,b:string,c:string,blabla:string")));
    b.setGroupByFields("c", "b");
    TupleMRConfig config = b.buildConf();
    config.getSerializationInfo();
    {
      List<SortElement> expectedCommon = new ArrayList<SortElement>();
      expectedCommon.add(new SortElement("c", Order.ASC, Criteria.NullOrder.NULL_SMALLEST));
      expectedCommon.add(new SortElement("b", Order.ASC, Criteria.NullOrder.NULL_SMALLEST));
      Assert.assertEquals(new Criteria(expectedCommon), config.getCommonCriteria());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRepeatedAliases() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields
        .parse("a:int,b:string,c:string,blabla:string")));
    b.setFieldAliases("schema1", new Aliases().add("bx", "b").add("bx", "c"));

  }

  @Test(expected = TupleMRException.class)
  public void testAliasesUnknownSchema() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields
        .parse("a:int,b:string,c:string,blabla:string")));
    b.setFieldAliases("schemaX", new Aliases().add("bx", "b"));
  }


  @Test(expected = TupleMRException.class)
  public void testSortFieldWithDifferentTypes1() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("a:int,b:boolean")));
    b.setGroupByFields("a");
    // not allowed to sort in common order by a field that has different types
    // even after source order
    // it can be confusing
    b.setOrderBy(new OrderBy().add("a", Order.ASC).addSchemaOrder(Order.DESC)
        .add("b", Order.DESC));
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testSortFieldWithDifferentTypes2() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("a:int,b:boolean")));
    b.setGroupByFields("a");
    b.setOrderBy(new OrderBy().add("a", Order.ASC).add("b", Order.DESC));
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testGroupByFieldWithDifferentTypes() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("a:int,b:boolean")));
    b.setGroupByFields("b", "a");
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testGroupByObjectDifferentSerialization() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    List<Field> fields = new ArrayList<Field>();
    fields.add(Field.createObject("my_object", A.class));
    b.addIntermediateSchema(new Schema("schema1", fields));
    fields.clear();
    Field avroField = Field.createObject("my_object", Object.class);
    avroField.setObjectSerialization(AvroFieldSerialization.class);
    fields.add(avroField);
    b.addIntermediateSchema(new Schema("schema2", fields));
    b.setGroupByFields("my_object");
    b.buildConf();
  }

  @Test
  public void testGroupByObjectSameSerialization() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    List<Field> fields = new ArrayList<Field>();
    fields.add(Field.createObject("my_object", A.class));
    b.addIntermediateSchema(new Schema("schema1", fields));
    b.addIntermediateSchema(new Schema("schema2", fields));
    b.setGroupByFields("my_object");
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testRepeatedSchemas() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("c:int,b:string")));
    b.setGroupByFields("b");
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testGroupByInvalidField() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.setGroupByFields("c");
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testCommonOrderPrefixGroupBy() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.setGroupByFields("a");
    b.setOrderBy(new OrderBy().add("b", Order.ASC));
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testCommonOrderPrefixGroupBy2() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string,c:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("a:int,b:string,d:string")));
    b.setGroupByFields("a", "b");
    b.setOrderBy(new OrderBy().add("b", Order.ASC).addSchemaOrder(Order.DESC)
        .add("a", Order.DESC));
    b.buildConf();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNotRepeatedFieldsInSortBy() {
    new OrderBy().add("foo", Order.ASC).add("foo", Order.DESC);
  }

  @Test(expected = IllegalStateException.class)
  public void testNotRepeatedSourceOrderInSortBy() throws TupleMRException {
    new OrderBy().add("foo", Order.DESC).addSchemaOrder(Order.ASC).add("bar", Order.DESC)
        .addSchemaOrder(Order.DESC);
  }

  @Test(expected = TupleMRException.class)
  public void testNotAllowedSourceOrderInOneSource() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.setGroupByFields("a");
    b.setOrderBy(new OrderBy().add("a", Order.ASC).addSchemaOrder(Order.DESC));
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testNotAllowedSourceOrderInSecondaryOrder() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("c:int,b:string")));
    b.setGroupByFields("b");
    b.setOrderBy(new OrderBy().add("b", Order.DESC).addSchemaOrder(Order.DESC));
    b.setSpecificOrderBy("schema1",
        new OrderBy().add("a", Order.DESC).addSchemaOrder(Order.DESC)); // this
    // is
    // incorrect
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testSecondaryOrderExistingSource() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("c:int,b:string")));
    b.setGroupByFields("b");
    b.setOrderBy(new OrderBy().add("b", Order.DESC).addSchemaOrder(Order.DESC));
    b.setSpecificOrderBy("invented_schema", new OrderBy().add("a", Order.DESC)
        .addSchemaOrder(Order.DESC));
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testSecondaryOrderNotNull() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("c:int,b:string")));
    b.setGroupByFields("b");
    b.setOrderBy(new OrderBy().add("b", Order.DESC).addSchemaOrder(Order.DESC));
    b.setSpecificOrderBy("schema1", null);
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testSecondaryOrderNotEmpty() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("c:int,b:string")));
    b.setGroupByFields("b");
    b.setOrderBy(new OrderBy().add("b", Order.DESC).addSchemaOrder(Order.DESC));
    b.setSpecificOrderBy("schema1", new OrderBy());
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testCommonOrderNotNull() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.setGroupByFields("b");
    b.setOrderBy(null);
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testCommonOrderNotEmpty() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.setGroupByFields("b");
    b.setOrderBy(new OrderBy());
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testFieldsRepeatedInCommonAndSecondaryOrder() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("a:int,b:string")));
    b.setGroupByFields("b");
    b.setOrderBy(new OrderBy().add("b", Order.DESC).addSchemaOrder(Order.DESC));
    b.setSpecificOrderBy("schema1", new OrderBy().add("b", Order.ASC));
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testNeedToDeclareCommonOrderWhenSecondary() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("a:int,b:string")));
    b.setGroupByFields("b");
    b.setSpecificOrderBy("schema1", new OrderBy().add("a", Order.ASC));
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testSourceOrderPresentInCommonWhenSecondarySet() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("a:int,b:string")));
    b.setGroupByFields("b");
    b.setOrderBy(new OrderBy().add("b", Order.DESC));
    b.setSpecificOrderBy("schema1", new OrderBy().add("a", Order.ASC));
    b.buildConf();

  }

  @Test(expected = TupleMRException.class)
  public void testRollUpCantBeNull() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("a:int,b:string")));
    b.setGroupByFields("b");
    b.setOrderBy(new OrderBy().add("b", Order.DESC));
    b.setRollupFrom(null);
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testRollupPrefixGroupBy() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.setGroupByFields("b");
    b.setOrderBy(new OrderBy().add("b", Order.DESC));
    b.setRollupFrom(null);
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testRollupNeedsExplicitSortBy() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.setGroupByFields("b", "a");
    b.setRollupFrom("a");
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testSpecificSortingNotAllowedWithOneSource() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.setGroupByFields("a");
    b.setOrderBy(new OrderBy().add("a", Order.ASC));
    b.setSpecificOrderBy("schema1", new OrderBy().add("b", Order.ASC));
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testCustomPartitionFieldsNotEmpty() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.setGroupByFields("a");
    b.setOrderBy(new OrderBy().add("a", Order.ASC));
    b.setCustomPartitionFields();
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testCustomPartitionFieldsNotNull() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.setGroupByFields("a");
    b.setOrderBy(new OrderBy().add("a", Order.ASC));
    String[] array = null;
    b.setCustomPartitionFields(array);
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testCustomPartitionFieldsPresentInAllSources() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("a:int,b:string,c:long")));
    b.setGroupByFields("a");
    b.setOrderBy(new OrderBy().add("a", Order.ASC));
    b.setCustomPartitionFields("c");
    b.buildConf();
  }

  @Test(expected = TupleMRException.class)
  public void testCustomPartitionFieldsPresentWithSameType() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("a:int,b:long")));
    b.setGroupByFields("a");
    b.setOrderBy(new OrderBy().add("a", Order.ASC));
    b.setCustomPartitionFields("b");
    b.buildConf();
  }

  @Test
  public void testCustomPartition() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("b:string,a:int")));
    b.setGroupByFields("a");
    b.setOrderBy(new OrderBy().add("a", Order.ASC));
    b.setCustomPartitionFields("b");
    TupleMRConfig config = b.buildConf();
    System.out.println(config);
    SerializationInfo serInfo = config.getSerializationInfo();
    int[] indexes0 = serInfo.getFieldsToPartition(0);
    int[] indexes1 = serInfo.getFieldsToPartition(1);
    Assert.assertArrayEquals(new int[]{1}, indexes0);
    Assert.assertArrayEquals(new int[]{0}, indexes1);
  }

  @Ignore
  @Test(expected = UnsupportedOperationException.class)
  public void testNotMutableConfig() throws TupleMRException {
    TupleMRConfigBuilder b = new TupleMRConfigBuilder();
    b.addIntermediateSchema(new Schema("schema1", Fields.parse("a:int,b:string")));
    b.addIntermediateSchema(new Schema("schema2", Fields.parse("b:string,a:int")));
    b.setGroupByFields("a");
    b.setOrderBy(new OrderBy().add("a", Order.ASC));
    b.setCustomPartitionFields("b");
    b.buildConf(); // TODO
  }


}
