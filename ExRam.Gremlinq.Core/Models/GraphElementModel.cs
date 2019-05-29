﻿using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using ExRam.Gremlinq.Core.Extensions;
using LanguageExt;

namespace ExRam.Gremlinq.Core
{
    public static class GraphElementModel
    {
        private sealed class GraphElementModelImpl : IGraphElementModel
        {
            public GraphElementModelImpl(IImmutableDictionary<Type, ElementMetadata> metaData)
            {
                Metadata = metaData;
            }

            public IImmutableDictionary<Type, ElementMetadata> Metadata { get; }
        }

        public static readonly IGraphElementModel Empty = new GraphElementModelImpl(ImmutableDictionary<Type, ElementMetadata>.Empty);

        private static readonly ConditionalWeakTable<IGraphElementModel, ConcurrentDictionary<Type, Option<string[]>>> DerivedLabels = new ConditionalWeakTable<IGraphElementModel, ConcurrentDictionary<Type, Option<string[]>>>();

        public static IGraphElementModel ConfigureLabels(this IGraphElementModel model, Func<Type, string, string> overrideTransformation)
        {
            return model.WithMetadata(_ => _.ToImmutableDictionary(
                kvp => kvp.Key,
                kvp => new ElementMetadata(overrideTransformation(kvp.Key, kvp.Value.Label))));
        }

        public static IGraphElementModel WithCamelCaseLabels(this IGraphElementModel model)
        {
            return model.ConfigureLabels((type, proposedLabel) => proposedLabel.ToCamelCase());
        }

        public static IGraphElementModel WithLowerCaseLabels(this IGraphElementModel model)
        {
            return model.ConfigureLabels((type, proposedLabel) => proposedLabel.ToLower());
        }

        public static Option<string[]> TryGetFilterLabels(this IGraphElementModel model, Type type)
        {
            return DerivedLabels
                .GetOrCreateValue(model)
                .GetOrAdd(
                    type,
                    closureType =>
                    {
                        var labels = model.Metadata
                            .Where(kvp => !kvp.Key.IsAbstract && closureType.IsAssignableFrom(kvp.Key))
                            .Select(kvp => kvp.Value.Label)
                            .OrderBy(x => x)
                            .ToArray();

                        return labels.Length == 0
                            ? default(Option<string[]>)
                            : labels.Length == model.Metadata.Count
                                ? Array.Empty<string>()
                                : labels;
                    });
        }

        internal static string[] GetValidFilterLabels(this IGraphElementModel model, Type type)
        {
            return model
                .TryGetFilterLabels(type)
                .IfNone(new[] { type.Name });   //TODO: What if type is abstract?
        }

        private static IGraphElementModel WithMetadata(this IGraphElementModel model, Func<IImmutableDictionary<Type, ElementMetadata>, IImmutableDictionary<Type, ElementMetadata>> transformation)
        {
            return new GraphElementModelImpl(
                transformation(model.Metadata));
        }
    }
}
