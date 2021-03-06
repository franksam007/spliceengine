/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;

import com.splicemachine.db.iapi.types.DataTypeUtilities;
import com.splicemachine.db.iapi.types.DateTimeDataValue;
import com.splicemachine.db.iapi.types.TypeId;

import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import com.splicemachine.db.iapi.sql.compile.TypeCompiler;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.util.ReuseFactory;

import java.sql.Types;
import java.util.List;
import java.util.Vector;

/**
 * This node represents a binary arithmetic operator, like + or *.
 *
 */

public final class BinaryArithmeticOperatorNode extends BinaryOperatorNode
{
    /**
     * Initializer for a BinaryArithmeticOperatorNode
     *
     * @param leftOperand	The left operand
     * @param rightOperand	The right operand
     */

    public void init(
            Object leftOperand,
            Object rightOperand)
    {
        super.init(leftOperand, rightOperand,
                ClassName.NumberDataValue, ClassName.NumberDataValue);
    }

    public void setNodeType(int nodeType)
    {
        String operator = null;
        String methodName = null;

        switch (nodeType)
        {
            case C_NodeTypes.BINARY_DIVIDE_OPERATOR_NODE:
                operator = TypeCompiler.DIVIDE_OP;
                methodName = "divide";
                break;

            case C_NodeTypes.BINARY_MINUS_OPERATOR_NODE:
                operator = TypeCompiler.MINUS_OP;
                methodName = "minus";
                break;

            case C_NodeTypes.BINARY_PLUS_OPERATOR_NODE:
                operator = TypeCompiler.PLUS_OP;
                methodName = "plus";
                break;

            case C_NodeTypes.BINARY_TIMES_OPERATOR_NODE:
                operator = TypeCompiler.TIMES_OP;
                methodName = "times";
                break;

            case C_NodeTypes.MOD_OPERATOR_NODE:
                operator = TypeCompiler.MOD_OP;
                methodName = "mod";
                break;

            default:
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT(
                            "Unexpected nodeType = " + nodeType);
                }
        }
        setOperator(operator);
        setMethodName(methodName);
        super.setNodeType(nodeType);
    }

    /**
     * Bind this operator
     *
     * @param fromList			The query's FROM list
     * @param subqueryList		The subquery list being built as we find SubqueryNodes
     * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
     *
     * @return	The new top of the expression tree.
     *
     * @exception StandardException		Thrown on error
     */
    @Override
    public ValueNode bindExpression( FromList	fromList, SubqueryList subqueryList, List<AggregateNode> aggregateVector) throws StandardException {
        if (leftOperand instanceof TimeSpanNode || rightOperand instanceof TimeSpanNode) {
            return bindTimeSpanOperation(fromList, subqueryList, aggregateVector);
        }
        super.bindExpression(fromList, subqueryList, aggregateVector);

        TypeId	leftType = leftOperand.getTypeId();
        TypeId	rightType = rightOperand.getTypeId();
        DataTypeDescriptor	leftDTS = leftOperand.getTypeServices();
        DataTypeDescriptor	rightDTS = rightOperand.getTypeServices();

        /* Do any implicit conversions from (long) (var)char. */
        if (leftType.isStringTypeId() && rightType.isNumericTypeId())
        {
            boolean nullableResult;
            nullableResult = leftDTS.isNullable() ||
                    rightDTS.isNullable();
            /* If other side is decimal/numeric, then we need to diddle
             * with the precision, scale and max width in order to handle
             * computations like:  1.1 + '0.111'
             */
            int precision = rightDTS.getPrecision();
            int scale	  = rightDTS.getScale();
            int maxWidth  = rightDTS.getMaximumWidth();

            if (rightType.isDecimalTypeId())
            {
                int charMaxWidth = leftDTS.getMaximumWidth();
                precision += (2 * charMaxWidth);
                scale += charMaxWidth;
                maxWidth = DataTypeUtilities.computeMaxWidth(precision, scale);
            }

            leftOperand = (ValueNode)
                    getNodeFactory().getNode(
                            C_NodeTypes.CAST_NODE,
                            leftOperand,
                            new DataTypeDescriptor(rightType, precision,
                                    scale, nullableResult,
                                    maxWidth),
                            getContextManager());
            ((CastNode) leftOperand).bindCastNodeOnly();
        }
        else if (rightType.isStringTypeId() && leftType.isNumericTypeId())
        {
            boolean nullableResult;
            nullableResult = leftDTS.isNullable() ||
                    rightDTS.isNullable();
            /* If other side is decimal/numeric, then we need to diddle
             * with the precision, scale and max width in order to handle
             * computations like:  1.1 + '0.111'
             */
            int precision = leftDTS.getPrecision();
            int scale	  = leftDTS.getScale();
            int maxWidth  = leftDTS.getMaximumWidth();

            if (leftType.isDecimalTypeId())
            {
                int charMaxWidth = rightDTS.getMaximumWidth();
                precision += (2 * charMaxWidth);
                scale += charMaxWidth;
                maxWidth = DataTypeUtilities.computeMaxWidth(precision, scale);
            }

            rightOperand =  (ValueNode)
                    getNodeFactory().getNode(
                            C_NodeTypes.CAST_NODE,
                            rightOperand,
                            new DataTypeDescriptor(leftType, precision,
                                    scale, nullableResult,
                                    maxWidth),
                            getContextManager());
            ((CastNode) rightOperand).bindCastNodeOnly();
        }

        /*
         ** Set the result type of this operator based on the operands.
         ** By convention, the left operand gets to decide the result type
         ** of a binary operator.
         */
        setType(leftOperand.getTypeCompiler().
                resolveArithmeticOperation(
                        leftOperand.getTypeServices(),
                        rightOperand.getTypeServices(),
                        operator
                )
        );

        return this;
    }

    private ValueNode bindTimeSpanOperation(FromList fromList,
                                            SubqueryList subqueryList,
                                            List<AggregateNode> aggregateVector) throws StandardException {
        if (leftOperand instanceof TimeSpanNode && rightOperand instanceof TimeSpanNode) {
            throw StandardException.newException(SQLState.LANG_INVALID_TIME_SPAN_OPERATION);
        }
        ValueNode base;
        TimeSpanNode timespan;
        if (leftOperand instanceof TimeSpanNode) {
            timespan = (TimeSpanNode) leftOperand;
            rightOperand = rightOperand.bindExpression(fromList, subqueryList, aggregateVector);
            base = rightOperand;
        } else {
            timespan = (TimeSpanNode) rightOperand;
            leftOperand = leftOperand.bindExpression(fromList, subqueryList, aggregateVector);
            base = leftOperand;
        }
        if (!"+".equals(operator) && !"-".equals(operator) || !base.getTypeId().isDateTimeTimeStampTypeId()) {
            throw StandardException.newException(SQLState.LANG_INVALID_TIME_SPAN_OPERATION);
        }
        if (base.getTypeId().getJDBCTypeId() == Types.DATE) {
            String function;
            Vector<ValueNode> parameterList = new Vector<>();
            switch (timespan.getUnit()) {
                case DateTimeDataValue.DAY_INTERVAL:
                    function = "ADD_DAYS";
                    break;
                case DateTimeDataValue.MONTH_INTERVAL:
                    function = "ADD_MONTHS";
                    break;
                case DateTimeDataValue.YEAR_INTERVAL:
                    function = "ADD_YEARS";
                    break;
                default:
                    throw StandardException.newException(SQLState.LANG_INVALID_TIME_SPAN_OPERATION,
                            timespan.getUnit());
            }
            MethodCallNode methodNode = (MethodCallNode) getNodeFactory().getNode(
                    C_NodeTypes.STATIC_METHOD_CALL_NODE,
                    getNodeFactory().getNode(
                            C_NodeTypes.TABLE_NAME,
                            null,
                            function,
                            getContextManager()
                    ),
                    null,
                    getContextManager()
            );
            parameterList.addElement(base);
            ValueNode value = timespan.getValue();
            if ("-".equals(operator)) {
                value = (ValueNode) getNodeFactory().getNode(
                        C_NodeTypes.UNARY_MINUS_OPERATOR_NODE,
                        value,
                        getContextManager());
            }
            parameterList.addElement(value);
            methodNode.addParms(parameterList);
            return ((ValueNode) getNodeFactory().getNode(
                    C_NodeTypes.JAVA_TO_SQL_VALUE_NODE,
                    methodNode,
                    getContextManager())).bindExpression(fromList, subqueryList, aggregateVector);
        } else if (base.getTypeId().getJDBCTypeId() == Types.TIMESTAMP) {
            ValueNode value = null;
            ValueNode intervalType = (ValueNode) getNodeFactory().getNode( C_NodeTypes.INT_CONSTANT_NODE,
                    ReuseFactory.getInteger(timespan.getUnit()),
                    getContextManager());

            value = timespan.getValue();
            if ("-".equals(operator)) {
                value = (ValueNode) getNodeFactory().getNode(
                        C_NodeTypes.UNARY_MINUS_OPERATOR_NODE,
                        value,
                        getContextManager());
            }
            return ((ValueNode) getNodeFactory().getNode( C_NodeTypes.TIMESTAMP_ADD_FN_NODE,
                    base,
                    intervalType,
                    value,
                    ReuseFactory.getInteger( TernaryOperatorNode.TIMESTAMPADD),
                    null,
                    getContextManager())).bindExpression(fromList, subqueryList, aggregateVector);

        }
        throw StandardException.newException(SQLState.LANG_INVALID_TIME_SPAN_OPERATION);
    }

    @Override
    public double getBaseOperationCost() throws StandardException {
        double localCost;
        switch (operator) {
            case TypeCompiler.TIMES_OP:
                localCost = SIMPLE_OP_COST * MULTIPLICATION_COST_FACTOR;
                break;
            case TypeCompiler.DIVIDE_OP:
            case TypeCompiler.MOD_OP:
                localCost = SIMPLE_OP_COST * DIV_COST_FACTOR;
                break;
            case TypeCompiler.PLUS_OP:
            case TypeCompiler.MINUS_OP:
            default:
                localCost = SIMPLE_OP_COST;
                break;
        }
        if (leftOperand.getTypeId().isFloatingPointTypeId() || rightOperand.getTypeId().isFloatingPointTypeId()) {
            localCost *= FLOAT_OP_COST_FACTOR;
        }
        return localCost + getChildrenCost() + SIMPLE_OP_COST * FN_CALL_COST_FACTOR;
    }
}
