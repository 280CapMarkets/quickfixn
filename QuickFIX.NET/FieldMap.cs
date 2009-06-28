﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using QuickFIX.NET;

namespace QuickFIX.NET
{
    public abstract class FieldMap
    {
        /// <summary>
        /// Default constructor
        /// </summary>
        public FieldMap()
        {
            this._fields = new Dictionary<int, Fields.IField>();
            this._groups = new Dictionary<int, List<Group>>();
        }

        /// <summary>
        /// Constructor with field order
        /// </summary>
        /// <param name="fieldOrd"></param>
        public FieldMap( int[] fieldOrd )
        {
            this._fields = new Dictionary<int, Fields.IField>();
            this._fieldOrder = fieldOrd;
            this._groups = new Dictionary<int, List<Group>>();
        }

        /// <summary>
        /// FieldOrder Property
        /// order of field tags as an integer array
        /// </summary>
        public int[] FieldOrder
        {
            get { return _fieldOrder; }
            private set { _fieldOrder = value; }
        }

        /// <summary>
        /// QuickFIX-CPP compat, see FieldOrder property
        /// </summary>
        /// <returns>field order integer array</returns>
        public int[] getFieldOrder()
        {
            return _fieldOrder;
        }

        /// <summary>
        /// set field in the fieldmap
        /// will overwrite field if it exists
        /// </summary>
        public void setField(Fields.IField field )
        {
            _fields[field.Tag] = field;
        }

        /// <summary>
        /// Set field with overwrite flag
        /// </summary>
        /// <param name="field"></param>
        /// <param name="overwrite">will overwrite wxisting field if set to true</param>
        public void setField(Fields.IField field, Boolean overwrite )
        {
            if (_fields.ContainsKey(field.Tag) && overwrite.Equals(false))
                return;
            else
                setField(field);
        }

        public void getField(Fields.BooleanField field)
        {
            if (_fields.ContainsKey(field.Tag))
                field.Obj = ((Fields.BooleanField)_fields[field.Tag]).Obj;
            else
                throw new FieldNotFoundException(field.Tag);
        } 
        
        public void getField(Fields.StringField field)
        {
            if (_fields.ContainsKey(field.Tag))
                field.Obj = ((Fields.StringField)_fields[field.Tag]).Obj;
            else
                throw new FieldNotFoundException(field.Tag);
        }

        public void getField(Fields.CharField field)
        {
            if(_fields.ContainsKey(field.Tag))
                field.Obj = ((Fields.CharField)_fields[field.Tag]).Obj;
            else
                throw new FieldNotFoundException(field.Tag);
        }

        public void getField(Fields.IntField field)
        {
            if (_fields.ContainsKey(field.Tag))
                field.Obj = ((Fields.IntField)_fields[field.Tag]).Obj;
            else
                throw new FieldNotFoundException(field.Tag);
        }

        public void getField(Fields.DecimalField field)
        {
            if (_fields.ContainsKey(field.Tag))
                field.Obj = ((Fields.DecimalField)_fields[field.Tag]).Obj;
            else
                throw new FieldNotFoundException(field.Tag);
        }

        public void getField(Fields.DateTimeField field)
        {
            if (_fields.ContainsKey(field.Tag))
                field.Obj = ((Fields.DateTimeField)_fields[field.Tag]).Obj;
            else
                throw new FieldNotFoundException(field.Tag);
        }

        public void AddGroup(Group group)
        {
            if( !_groups.ContainsKey(group.Field) )
                _groups.Add(group.Field, new List<Group>());
            _groups[group.Field].Add(group);
        }

        /// <summary>
        /// Gets specific group instance
        /// </summary>
        /// <param name="num">num of group (starting at 1)</param>
        /// <param name="tag">tag of group</param>
        /// <returns></returns>
        public Group GetGroup( int num, int field )
        {
            if ( !_groups.ContainsKey(field) ) 
                throw new FieldNotFoundException(field);
            if ( num <= 0 ) 
                throw new FieldNotFoundException(field);
            if ( _groups[field].Count < num ) 
                throw new FieldNotFoundException(field);

            return _groups[field][num-1];
        }

        public void RemoveGroup(int num, int field)
        {
            if (!_groups.ContainsKey(field))
                throw new FieldNotFoundException(field);
            if (num <= 0)
                throw new FieldNotFoundException(field);
            if (_groups[field].Count < num)
                throw new FieldNotFoundException(field);
            
            if (_groups[field].Count.Equals(1))
                _groups.Remove(field);
            else
                _groups[field].RemoveAt(num);
        }


        /// <summary>
        /// getField without a type defaults to returning a string
        /// </summary>
        /// <param name="tag">fix tag</param>
        public string GetField(int tag)
        {
            if (_fields.ContainsKey(tag))
                return _fields[tag].ToString();
            else
                throw new FieldNotFoundException(tag);
        }

        #region Private Members
        private Dictionary<int, Fields.IField> _fields;
        private Dictionary<int, List<Group>> _groups;
        private int[] _fieldOrder;
        #endregion
    }
}