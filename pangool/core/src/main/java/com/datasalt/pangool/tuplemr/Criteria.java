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

import org.apache.hadoop.io.RawComparator;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Criteria specifies a sorting criteria defined by a list of
 * {@link SortElement} instances that contain a field name and its corresponding
 * order defined by {@link Criteria.Order}.
 * <p/>
 * In addition, the criteria allows to specify,for every field,
 * a custom comparator implementing {@link RawComparator}
 */
public class Criteria {

  public static enum Order {
    ASC("asc"), DESC("desc");

    private String abr;

    private Order(String abr) {
      this.abr = abr;
    }

    public String getAbreviation() {
      return abr;
    }
  }

  public static enum NullOrder {
    NULL_SMALLEST, NULL_BIGGEST
  }

  private List<SortElement> elements;

  public Criteria(List<SortElement> elements) {
    this.elements = Collections.unmodifiableList(elements);
  }

  public Criteria() {
  }

  public List<SortElement> getElements() {
    return elements;
  }

  /**
   * Basic {@link Criteria} element.
   * Just a structure containing field's name, order, and custom comparator.
   */
  public static class SortElement {
    private final String name;
    private final Order order;
    private final NullOrder nullOrder;
    private RawComparator<?> customComparator;

    public RawComparator<?> getCustomComparator() {
      return customComparator;
    }

    public void setCustomComparator(RawComparator<?> customComparator) {
      this.customComparator = customComparator;
    }

    public String getName() {
      return name;
    }

    public Order getOrder() {
      return order;
    }

    public NullOrder getNullOrder() {
      return nullOrder;
    }

    @Override
    public boolean equals(Object a) {
      if (!(a instanceof SortElement)) {
        return false;
      }

      SortElement that = (SortElement) a;
      RawComparator<?> thisc = this.getCustomComparator();
      RawComparator<?> thatc = this.getCustomComparator();

      if (thisc != null && thatc == null) {
        return false;
      } else if (thisc != null && !thisc.equals(thatc)) {
        return false;
      }
      return this.getName().equals(that.getName())
          && this.getOrder().equals(that.getOrder())
          && this.getNullOrder().equals(that.getNullOrder());
    }

    @Override
    public int hashCode() {
      return getName().hashCode();
    }

    public SortElement(String name, Order order, NullOrder nullOrder) {
      boolean exception = false;
      String param = "";
      if (name == null) {
        exception = true;
        param = "name";
      } else if (order == null) {
        exception = true;
        param = "order";
      } else if (nullOrder == null) {
        exception = true;
        param = "nullOrder";
      }
      if (exception) {
        throw new IllegalArgumentException("Parameter " + param + " cannot be null");
      }
      this.name = name;
      this.order = order;
      this.nullOrder = nullOrder;
    }

    public SortElement(String name, Order order, NullOrder nullOrder, RawComparator<?> comparator) {
      this(name, order, nullOrder);
      this.customComparator = comparator;
    }

    void toJson(JsonGenerator gen) throws IOException {
      gen.writeStartObject();
      gen.writeStringField("name", name);
      gen.writeStringField("order", order.toString());
      gen.writeStringField("nullOrder", nullOrder.toString());
      gen.writeEndObject();
    }

    static SortElement parse(JsonNode node) throws IOException {
      String name = node.get("name").getTextValue();
      Order order = Order.valueOf(node.get("order").getTextValue());
      NullOrder nullOrder = NullOrder.valueOf(node.get("nullOrder").getTextValue());
      return new SortElement(name, order, nullOrder);
    }

    @Override
    public String toString() {
      try {
        StringWriter w = new StringWriter();
        JsonGenerator gen = new JsonFactory().createJsonGenerator(w);
        toJson(gen);
        gen.flush();
        return w.toString();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public String toString() {
    try {
      StringWriter w = new StringWriter();
      JsonGenerator gen = new JsonFactory().createJsonGenerator(w);
      toJson(gen);
      gen.flush();
      return w.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void toJson(JsonGenerator gen) throws IOException {
    gen.writeStartArray();
    for (SortElement s : elements) {
      s.toJson(gen);
    }
    gen.writeEndArray();
  }

  public static Criteria parse(JsonNode node) throws IOException {
    Iterator<JsonNode> elements = node.getElements();
    List<SortElement> sorts = new ArrayList<SortElement>();
    while (elements.hasNext()) {
      sorts.add(SortElement.parse(elements.next()));
    }
    return new Criteria(sorts);
  }

  @Override
  public boolean equals(Object a) {
    if (a instanceof Criteria) {
      return getElements().equals(((Criteria) a).getElements());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return getElements().hashCode();
  }
}
