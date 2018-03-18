﻿using System;

namespace ExRam.Gremlinq
{
    public abstract class StepLabel : IGremlinSerializable
    {
        public abstract GroovyExpressionBuilder Serialize(GroovyExpressionBuilder builder, IParameterCache parameterCache);
    }

    public class StepLabel<TElement> : StepLabel
    {
        public override GroovyExpressionBuilder Serialize(GroovyExpressionBuilder builder, IParameterCache parameterCache)
        {
            return builder.AppendConstant(this, parameterCache);
        }

        public static bool operator ==(TElement a, StepLabel<TElement> b)
        {
            throw new NotImplementedException("Only for expressions.");
        }

        public static bool operator !=(TElement a, StepLabel<TElement> b)
        {
            throw new NotImplementedException("Only for expressions.");
        }
    }
}
