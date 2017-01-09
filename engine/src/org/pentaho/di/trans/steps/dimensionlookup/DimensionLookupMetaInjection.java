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

package org.pentaho.di.trans.steps.dimensionlookup;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleTransException;
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
public class DimensionLookupMetaInjection implements StepMetaInjectionInterface {

  public enum Entry implements StepMetaInjectionEntryInterface {
	  
	  UPDATE( ValueMetaInterface.TYPE_STRING, "Update the dimension or just lookup? (Y/N)"),
	  DATABASE_META(ValueMetaInterface.TYPE_STRING, "The database connection"),
	  SCHEMA_NAME(ValueMetaInterface.TYPE_STRING, "The lookup schema name"),
	  TABLE_NAME(ValueMetaInterface.TYPE_STRING, "The lookup table"),
	  COMMIT_SIZE( ValueMetaInterface.TYPE_STRING, "The number of rows between commits" ),
	  PRELOADING_CACHE( ValueMetaInterface.TYPE_STRING, "Preloading cache? (Y/N)"),
	  CACHE_SIZE( ValueMetaInterface.TYPE_STRING, "The size of the cache in ROWS : -1 means: not set, 0 means: cache all"),
	  SURROGATE_KEY( ValueMetaInterface.TYPE_STRING, "Name of the technical key (surrogate key) field to return from the dimension" ),
	  KEY_RENAME( ValueMetaInterface.TYPE_STRING, "New name of the technical key field" ),
	  VERSION_FIELD( ValueMetaInterface.TYPE_STRING, "The name of the version field" ),
	  DATE_FIELD( ValueMetaInterface.TYPE_STRING, "The field to use for date range lookup in the dimension" ),
	  DATE_FROM( ValueMetaInterface.TYPE_STRING, "The 'from' field of the date range in the dimension" ),
	  DATE_TO( ValueMetaInterface.TYPE_STRING, "The 'to' field of the date range in the dimension" ),
	  USING_START_DATE_ALTERNATIVE( ValueMetaInterface.TYPE_STRING, "Flag to indicate we're going to use an alternative start date (Y/N)" ),
	  START_DATE_ALTERNATIVE( ValueMetaInterface.TYPE_STRING, "The type of alternative" ),
	  START_DATE_FIELD_NAME( ValueMetaInterface.TYPE_STRING, "The field name in case we select the column value option as an alternative start date" ),
	  
	  KEY_FIELDS( ValueMetaInterface.TYPE_NONE, "The key fields" ),
      KEY_FIELD( ValueMetaInterface.TYPE_NONE, "Key field" ),
      KEY_LOOKUP( ValueMetaInterface.TYPE_STRING, "Fields in the dimension to use for lookup" ),
      KEY_STREAM( ValueMetaInterface.TYPE_STRING, "Fields used to look up a value in the dimension" ),

      VALUE_FIELDS( ValueMetaInterface.TYPE_NONE, "The value fields" ),
      VALUE_FIELD( ValueMetaInterface.TYPE_NONE, "Value field" ),
      FIELD_LOOKUP( ValueMetaInterface.TYPE_STRING, "Fields in the dimension to update or retrieve" ),
      FIELD_STREAM( ValueMetaInterface.TYPE_STRING, "Fields containing the values in the input stream to update the dimension with" ),
	  FIELD_UPDATE( ValueMetaInterface.TYPE_STRING, "The type of update to perform on the fields: insert, update, punch-through" );

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

  private DimensionLookupMeta meta;

  public DimensionLookupMetaInjection( DimensionLookupMeta meta ) {
    this.meta = meta;
  }

  @Override
  public List<StepInjectionMetaEntry> getStepInjectionMetadataEntries() throws KettleException {
    List<StepInjectionMetaEntry> all = new ArrayList<StepInjectionMetaEntry>();
    
    Entry[] topEntries =
      new Entry[] {
    	Entry.UPDATE, Entry.DATABASE_META, Entry.SCHEMA_NAME, Entry.TABLE_NAME, Entry.COMMIT_SIZE, Entry.PRELOADING_CACHE, 
    	Entry.CACHE_SIZE, Entry.SURROGATE_KEY, Entry.KEY_RENAME, Entry.VERSION_FIELD, Entry.DATE_FIELD, Entry.DATE_FROM, Entry.DATE_TO, 
    	Entry.USING_START_DATE_ALTERNATIVE, Entry.START_DATE_ALTERNATIVE, Entry.START_DATE_FIELD_NAME };
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
    Entry[] keyFieldsEntries = new Entry[] { Entry.KEY_LOOKUP, Entry.KEY_STREAM };
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
    Entry[] valueFieldsEntries = new Entry[] { Entry.FIELD_LOOKUP, Entry.FIELD_STREAM, Entry.FIELD_UPDATE };
    for ( Entry entry : valueFieldsEntries ) {
      StepInjectionMetaEntry metaEntry =
        new StepInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
      valueFieldEntry.getDetails().add( metaEntry );
    }

    return all;
  }

  @Override
  public void injectStepMetadataEntries( List<StepInjectionMetaEntry> all ) throws KettleException {

    List<String> keyLookups = new ArrayList<String>();
    List<String> keyStreams = new ArrayList<String>();
    List<String> fieldLookups = new ArrayList<String>();
    List<String> fieldStreams = new ArrayList<String>();
    List<String> fieldUpdates = new ArrayList<String>();

    // Parse the fields, inject into the meta class..
    //
    for ( StepInjectionMetaEntry lookFields : all ) {
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry == null ) {
        continue;
      }

      String lookValue = (String) lookFields.getValue();
      switch ( fieldsEntry ) {
      	case UPDATE:
          meta.setUpdate( "Y".equalsIgnoreCase( lookValue ) );
          break;
      	case DATABASE_META:
      		meta.setDatabaseMeta(DatabaseMeta.findDatabase(meta.getDatabaseMetas(), lookValue));
            break;
      	case SCHEMA_NAME:
            meta.setSchemaName( lookValue );
            break;
      	case TABLE_NAME:
            meta.setTableName( lookValue );
            break;
      	case COMMIT_SIZE:
            meta.setCommitSize( Const.toInt( lookValue, 0 ) );
            break;
      	case PRELOADING_CACHE:
            meta.setPreloadingCache( "Y".equalsIgnoreCase( lookValue ) );
            break;
      	case CACHE_SIZE:
            meta.setCacheSize( Const.toInt( lookValue, 1 ) );
            break;
      	case SURROGATE_KEY:
            meta.setKeyField( lookValue );
            break;
      	case KEY_RENAME:
            meta.setKeyRename( lookValue );
            break;
      	case VERSION_FIELD:
            meta.setVersionField( lookValue );
            break;
      	case DATE_FIELD:
            meta.setDateField( lookValue );
            break;
      	case DATE_FROM:
            meta.setDateFrom( lookValue );
            break;
      	case DATE_TO:
            meta.setDateTo( lookValue );
            break;
      	case USING_START_DATE_ALTERNATIVE:
            meta.setUsingStartDateAlternative( "Y".equalsIgnoreCase( lookValue ) );
            break;
      	case START_DATE_ALTERNATIVE:
            meta.setStartDateAlternative( getIndex(lookValue, meta.startDateAlternativeCodes) );
            break;
      	case START_DATE_FIELD_NAME:
            meta.setStartDateFieldName( lookValue );
            break;
            
        case KEY_FIELDS:
          for ( StepInjectionMetaEntry lookField : lookFields.getDetails() ) {
            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
            if ( fieldEntry == Entry.KEY_FIELD ) {
            	List<StepInjectionMetaEntry> entries = lookField.getDetails();
                for ( StepInjectionMetaEntry entry : entries ) {
                  Entry metaEntry = Entry.findEntry( entry.getKey() );
                  if ( metaEntry != null ) {
                    String value = (String) entry.getValue();
                    if (metaEntry == Entry.KEY_LOOKUP)
                    	keyLookups.add(value);
                    if (metaEntry == Entry.KEY_STREAM)
                    	keyStreams.add(value);
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
                    if (metaEntry == Entry.FIELD_LOOKUP)
                    	fieldLookups.add(value);
                    if (metaEntry == Entry.FIELD_STREAM)
                    	fieldStreams.add(value);
                    if (metaEntry == Entry.FIELD_UPDATE)
                    	fieldUpdates.add(value);
                  }
                }
              }
            }
            break;
        default:
          break;
      }
    }

    // Pass the grid to the step metadata
    //
    if ( keyLookups.size() > 0 ) {
      meta.setKeyLookup( keyLookups.toArray( new String[keyLookups.size()] ) );
    }
    if ( keyStreams.size() > 0 ) {
      meta.setKeyStream( keyStreams.toArray( new String[keyStreams.size()] ) );
    }
    if ( fieldLookups.size() > 0 ) {
	    meta.setFieldLookup( fieldLookups.toArray( new String[fieldLookups.size()] ) );
	  }
    if ( fieldStreams.size() > 0 ) {
        meta.setFieldStream( fieldStreams.toArray( new String[fieldStreams.size()] ) );
      }
    if ( fieldUpdates.size() > 0 ) {
    	int[] fu = new int[fieldUpdates.size()];
    	for (int i = 0; i < fieldUpdates.size(); i++) {
			fu[i] = getIndex(fieldUpdates.get(i),meta.typeCodes);
		}
    	meta.setFieldUpdate(fu);
    }
  }
  
  private int getIndex(String str, String[] list){
	  int i = 0;
	  for (; i < list.length; i++) {
		if (str.equals(list[i])){
			break;
		}
	  }
	  if (i == list.length) {
		  return 0;
	  } else {
		  return i;
	  }
  }

  public List<StepInjectionMetaEntry> extractStepMetadataEntries() throws KettleException {
    List<StepInjectionMetaEntry> list = new ArrayList<StepInjectionMetaEntry>();

    list.add( StepInjectionUtil.getEntry( Entry.UPDATE, meta.isUpdate() ) );
    list.add( StepInjectionUtil.getEntry( Entry.DATABASE_META, meta.getDatabaseMeta() ) );
    list.add( StepInjectionUtil.getEntry( Entry.SCHEMA_NAME, meta.getSchemaName() ) );
    list.add( StepInjectionUtil.getEntry( Entry.TABLE_NAME, meta.getTableName() ) );
    list.add( StepInjectionUtil.getEntry( Entry.COMMIT_SIZE, meta.getCommitSize() ) );
    list.add( StepInjectionUtil.getEntry( Entry.PRELOADING_CACHE, meta.isPreloadingCache() ) );
    list.add( StepInjectionUtil.getEntry( Entry.CACHE_SIZE, meta.getCacheSize() ) );
    list.add( StepInjectionUtil.getEntry( Entry.SURROGATE_KEY, meta.getKeyField() ) );
    list.add( StepInjectionUtil.getEntry( Entry.KEY_RENAME, meta.getKeyRename() ) );
    list.add( StepInjectionUtil.getEntry( Entry.VERSION_FIELD, meta.getVersionField() ) );
    list.add( StepInjectionUtil.getEntry( Entry.DATE_FIELD, meta.getDateField() ) );
    list.add( StepInjectionUtil.getEntry( Entry.DATE_FROM, meta.getDateFrom() ) );
    list.add( StepInjectionUtil.getEntry( Entry.DATE_TO, meta.getDateTo() ) );
    list.add( StepInjectionUtil.getEntry( Entry.USING_START_DATE_ALTERNATIVE, meta.isUsingStartDateAlternative() ) );
    list.add( StepInjectionUtil.getEntry( Entry.START_DATE_ALTERNATIVE, meta.getStartDateAlternative() ) );
    list.add( StepInjectionUtil.getEntry( Entry.START_DATE_FIELD_NAME, meta.getStartDateFieldName() ) );
    
    StepInjectionMetaEntry keyFieldsEntry = StepInjectionUtil.getEntry( Entry.KEY_FIELDS );
    list.add( keyFieldsEntry );
    for ( int i = 0; i < meta.getKeyLookup().length; i++ ) {
    	StepInjectionMetaEntry fieldEntry = StepInjectionUtil.getEntry( Entry.KEY_FIELD );
        List<StepInjectionMetaEntry> details = fieldEntry.getDetails();
        details.add( StepInjectionUtil.getEntry( Entry.KEY_LOOKUP, meta.getKeyLookup()[i] ) );
        details.add( StepInjectionUtil.getEntry( Entry.KEY_STREAM, meta.getKeyStream()[i] ) );
        keyFieldsEntry.getDetails().add( fieldEntry );
    }
    
    StepInjectionMetaEntry valueFieldsEntry = StepInjectionUtil.getEntry( Entry.VALUE_FIELDS );
    list.add( valueFieldsEntry );
    for ( int i = 0; i < meta.getFieldLookup().length; i++ ) {
    	StepInjectionMetaEntry fieldEntry = StepInjectionUtil.getEntry( Entry.VALUE_FIELD );
        List<StepInjectionMetaEntry> details = fieldEntry.getDetails();
        details.add( StepInjectionUtil.getEntry( Entry.FIELD_LOOKUP, meta.getFieldLookup()[i] ) );
        details.add( StepInjectionUtil.getEntry( Entry.FIELD_STREAM, meta.getFieldStream()[i] ) );
        details.add( StepInjectionUtil.getEntry( Entry.FIELD_UPDATE, meta.getFieldUpdate()[i] ) );
        valueFieldsEntry.getDetails().add( fieldEntry );
    }

    return list;
  }

  public DimensionLookupMeta getMeta() {
    return meta;
  }
}
