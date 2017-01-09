/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.mergerows;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.StepInjectionMetaEntry;
import org.pentaho.di.trans.step.StepInjectionUtil;
import org.pentaho.di.trans.step.StepMetaInjectionEntryInterface;
import org.pentaho.di.trans.step.StepMetaInjectionInterface;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMetaInjection.Entry;

/**
 * This takes care of the external metadata injection into the TableOutputMeta class
 *
 * @author Chris
 */
public class MergeRowsMetaInjection implements StepMetaInjectionInterface {

  public enum Entry implements StepMetaInjectionEntryInterface {

	  FLAG_FIELD( ValueMetaInterface.TYPE_STRING, "the flagField" ),

      KEY_FIELDS( ValueMetaInterface.TYPE_NONE, "The key fields" ),
      KEY_FIELD( ValueMetaInterface.TYPE_NONE, "Key field" ),
      KEY_NAME( ValueMetaInterface.TYPE_STRING, "Key name" ),
      
      VALUE_FIELDS( ValueMetaInterface.TYPE_NONE, "The value fields" ),
      VALUE_FIELD( ValueMetaInterface.TYPE_NONE, "Value field" ),
      VALUE_NAME( ValueMetaInterface.TYPE_STRING, "Value Name" );

    private int valueType;
    private String description;

    private Entry( int valueType, String description ) {
      this.valueType = valueType;
      this.description = description;
    }

    /**
     * @return the valueType
     */
    public int getValueType() {
      return valueType;
    }

    /**
     * @return the description
     */
    public String getDescription() {
      return description;
    }

    public static Entry findEntry( String key ) {
      return Entry.valueOf( key );
    }
  }

  private MergeRowsMeta meta;

  public MergeRowsMetaInjection( MergeRowsMeta meta ) {
    this.meta = meta;
  }

  @Override
  public List<StepInjectionMetaEntry> getStepInjectionMetadataEntries() throws KettleException {
    List<StepInjectionMetaEntry> all = new ArrayList<StepInjectionMetaEntry>();

    Entry[] topEntries =
      new Entry[] {
        Entry.FLAG_FIELD };
    for ( Entry topEntry : topEntries ) {
      all.add( new StepInjectionMetaEntry( topEntry.name(), topEntry.getValueType(), topEntry.getDescription() ) );
    }

    // The fields
    //
    StepInjectionMetaEntry keyFieldsEntry =
      new StepInjectionMetaEntry(
        Entry.KEY_FIELDS.name(), ValueMetaInterface.TYPE_NONE, Entry.KEY_FIELDS.description );
    all.add( keyFieldsEntry );
    StepInjectionMetaEntry keyFieldEntry =
      new StepInjectionMetaEntry(
        Entry.KEY_FIELD.name(), ValueMetaInterface.TYPE_NONE, Entry.KEY_FIELD.description );
    keyFieldsEntry.getDetails().add( keyFieldEntry );
    Entry[] keyFieldsEntries = new Entry[] { Entry.KEY_NAME };
    for ( Entry entry : keyFieldsEntries ) {
      StepInjectionMetaEntry metaEntry =
        new StepInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
      keyFieldEntry.getDetails().add( metaEntry );
    }

    StepInjectionMetaEntry valueFieldsEntry =
      new StepInjectionMetaEntry(
        Entry.VALUE_FIELDS.name(), ValueMetaInterface.TYPE_NONE, Entry.VALUE_FIELDS.description );
    all.add( valueFieldsEntry );
    StepInjectionMetaEntry valueFieldEntry =
      new StepInjectionMetaEntry(
        Entry.VALUE_FIELD.name(), ValueMetaInterface.TYPE_NONE, Entry.VALUE_FIELD.description );
    valueFieldsEntry.getDetails().add( valueFieldEntry );   
    Entry[] valueFieldsEntries = new Entry[] { Entry.VALUE_NAME };
    for ( Entry entry : valueFieldsEntries ) {
      StepInjectionMetaEntry metaEntry =
        new StepInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
      valueFieldEntry.getDetails().add( metaEntry );
    }

    return all;
  }

  @Override
  public void injectStepMetadataEntries( List<StepInjectionMetaEntry> all ) throws KettleException {

    List<String> keyFields = new ArrayList<String>();
    List<String> valueFields = new ArrayList<String>();

    // Parse the fields, inject into the meta class..
    //
    for ( StepInjectionMetaEntry lookFields : all ) {
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry == null ) {
        continue;
      }

      String lookValue = (String) lookFields.getValue();
      switch ( fieldsEntry ) {
        case KEY_FIELDS:
          for ( StepInjectionMetaEntry lookField : lookFields.getDetails() ) {
            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
            if ( fieldEntry == Entry.KEY_FIELD ) {
            	List<StepInjectionMetaEntry> entries = lookField.getDetails();
                for ( StepInjectionMetaEntry entry : entries ) {
                  Entry metaEntry = Entry.findEntry( entry.getKey() );
                  if ( metaEntry != null ) {
                    String value = (String) entry.getValue();
                    if (metaEntry == Entry.KEY_NAME)
                    	keyFields.add(value);
                  }
                }
            }
          }
          break;
        case VALUE_FIELDS:
            for ( StepInjectionMetaEntry lookField : lookFields.getDetails() ) {
              Entry fieldEntry = Entry.findEntry( lookField.getKey() );
              if ( fieldEntry == Entry.VALUE_FIELD ) {
              	List<StepInjectionMetaEntry> entries = lookField.getDetails();
                for ( StepInjectionMetaEntry entry : entries ) {
                  Entry metaEntry = Entry.findEntry( entry.getKey() );
                  if ( metaEntry != null ) {
                    String value = (String) entry.getValue();
                    if (metaEntry == Entry.VALUE_NAME)
                    	valueFields.add(value);
                  }
                }
              }
            }
            break;
        case FLAG_FIELD:
          meta.setFlagField( lookValue );
          break;
        default:
          break;
      }
    }

    // Pass the grid to the step metadata
    //
    if ( keyFields.size() > 0 ) {
      meta.setKeyFields( keyFields.toArray( new String[keyFields.size()] ) );
    }
    if ( valueFields.size() > 0 ) {
      meta.setValueFields( valueFields.toArray( new String[valueFields.size()] ) );
    }
  }

  public List<StepInjectionMetaEntry> extractStepMetadataEntries() throws KettleException {
    List<StepInjectionMetaEntry> list = new ArrayList<StepInjectionMetaEntry>();

    list.add( StepInjectionUtil.getEntry( Entry.FLAG_FIELD, meta.getFlagField() ) );

    StepInjectionMetaEntry keyFieldsEntry = StepInjectionUtil.getEntry( Entry.KEY_FIELDS );
    list.add( keyFieldsEntry );
    for ( int i = 0; i < meta.getKeyFields().length; i++ ) {
    	StepInjectionMetaEntry fieldEntry = StepInjectionUtil.getEntry( Entry.KEY_FIELD );
        List<StepInjectionMetaEntry> details = fieldEntry.getDetails();
        details.add( StepInjectionUtil.getEntry( Entry.KEY_NAME, meta.getKeyFields()[i] ) );
        keyFieldsEntry.getDetails().add( fieldEntry );
    }
    
    StepInjectionMetaEntry valueFieldsEntry = StepInjectionUtil.getEntry( Entry.VALUE_FIELDS );
    list.add( valueFieldsEntry );
    for ( int i = 0; i < meta.getValueFields().length; i++ ) {
    	StepInjectionMetaEntry fieldEntry = StepInjectionUtil.getEntry( Entry.VALUE_FIELD );
        List<StepInjectionMetaEntry> details = fieldEntry.getDetails();
        details.add( StepInjectionUtil.getEntry( Entry.VALUE_NAME, meta.getKeyFields()[i] ) );
        valueFieldsEntry.getDetails().add( fieldEntry );
    }

    return list;
  }

  public MergeRowsMeta getMeta() {
    return meta;
  }
}
