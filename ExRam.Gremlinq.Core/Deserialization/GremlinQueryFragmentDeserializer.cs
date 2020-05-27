﻿using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq.Expressions;

namespace ExRam.Gremlinq.Core
{
    public static class GremlinQueryFragmentDeserializer
    {
        private sealed class GremlinQueryFragmentDeserializerImpl : IGremlinQueryFragmentDeserializer
        {
            private readonly IImmutableDictionary<Type, Delegate> _dict;
            private readonly ConcurrentDictionary<(Type staticType, Type actualType), Delegate?> _fastDict = new ConcurrentDictionary<(Type staticType, Type actualType), Delegate?>();

            public GremlinQueryFragmentDeserializerImpl(IImmutableDictionary<Type, Delegate> dict)
            {
                _dict = dict;
            }

            public object? TryDeserialize<TSerialized>(TSerialized serializedData, Type fragmentType, IGremlinQueryEnvironment environment)
            {
                return TryGetDeserializer(typeof(TSerialized), serializedData.GetType()) is Func<TSerialized, Type, IGremlinQueryEnvironment, object?> del
                    ? del(serializedData, fragmentType, environment)
                    : serializedData;
            }

            public IGremlinQueryFragmentDeserializer Override<TSerialized>(Func<TSerialized, Type, IGremlinQueryEnvironment, Func<TSerialized, object?>, IGremlinQueryFragmentDeserializer, object?> deserializer)
            {
                return new GremlinQueryFragmentDeserializerImpl(
                    _dict.SetItem(
                        typeof(TSerialized),
                        InnerLookup(typeof(TSerialized)) is Func<object, Type, IGremlinQueryEnvironment, Func<object, object?>, IGremlinQueryFragmentDeserializer, object?> existingFragmentSerializer
                            ? new Func<object, Type, IGremlinQueryEnvironment, Func<object, object?>, IGremlinQueryFragmentDeserializer, object?>((fragment, type, env, baseSerializer, recurse) => deserializer((TSerialized)fragment, type, env, _ => existingFragmentSerializer(_!, type, env, baseSerializer, recurse), recurse))
                            : (fragment, type, env, baseSerializer, recurse) => deserializer((TSerialized)fragment, type, env, _ => baseSerializer(_!), recurse)));
            }

            private Delegate? TryGetDeserializer(Type staticType, Type actualType)
            {
                return _fastDict
                    .GetOrAdd(
                        (staticType, actualType),
                        (typeTuple, @this) =>
                        {
                            var (staticType, actualType) = typeTuple;

                            if (@this.InnerLookup(actualType) is { } del)
                            {
                                //return (TStatic serialized, Type fragmentType, IGremlinQueryEnvironment environment) => del((TActualType)serialized, fragmentType, environment, (TActual _) => _, @this);

                                var effectiveType = del.GetType().GetGenericArguments()[0];
                                var argument4Parameter = Expression.Parameter(effectiveType);

                                var serializedParameter = Expression.Parameter(staticType);
                                var fragmentTypeParameter = Expression.Parameter(typeof(Type));
                                var environmentParameter = Expression.Parameter(typeof(IGremlinQueryEnvironment));
                                
                                var effectiveTypeFunc = typeof(Func<,>).MakeGenericType(effectiveType, typeof(object));
                                var staticTypeFunc = typeof(Func<,,,>).MakeGenericType(staticType, typeof(Type), typeof(IGremlinQueryEnvironment), typeof(object));

                                var retCall = Expression.Invoke(
                                    Expression.Constant(del),
                                    Expression.Convert(
                                        serializedParameter,
                                        effectiveType),
                                    fragmentTypeParameter,
                                    environmentParameter,
                                    Expression.Lambda(
                                        effectiveTypeFunc,
                                        Expression.Convert(argument4Parameter, typeof(object)),
                                        argument4Parameter),
                                    Expression.Constant(@this));

                                return Expression
                                    .Lambda(
                                        staticTypeFunc,
                                        retCall,
                                        serializedParameter,
                                        fragmentTypeParameter,
                                        environmentParameter)
                                    .Compile();
                            }

                            return null;
                        },
                        this);
            }

            private Delegate? InnerLookup(Type actualType)
            {
                if (_dict.TryGetValue(actualType, out var ret))
                    return ret;

                foreach (var implementedInterface in actualType.GetInterfaces())
                {
                    if (InnerLookup(implementedInterface) is { } interfaceSerializer)
                        return interfaceSerializer;
                }

                if (actualType.BaseType is { } baseType)
                {
                    if (InnerLookup(baseType) is { } baseSerializer)
                        return baseSerializer;
                }

                return null;
            }
        }

        public static readonly IGremlinQueryFragmentDeserializer Identity = new GremlinQueryFragmentDeserializerImpl(ImmutableDictionary<Type, Delegate>.Empty);
    }
}