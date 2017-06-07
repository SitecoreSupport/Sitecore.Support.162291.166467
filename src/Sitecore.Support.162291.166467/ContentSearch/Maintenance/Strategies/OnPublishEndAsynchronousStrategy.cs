namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
  using Data.Archiving;
  using Jobs;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Diagnostics;
  using Sitecore.ContentSearch.Maintenance;
  using Sitecore.Data;
  using Sitecore.Data.Eventing.Remote;
  using Sitecore.Eventing;
  using Sitecore.Globalization;
  using System.Collections.Generic;
  using System.Linq;

  public class OnPublishEndAsynchronousStrategy : Sitecore.ContentSearch.Maintenance.Strategies.OnPublishEndAsynchronousStrategy
  {
    public OnPublishEndAsynchronousStrategy(string database) : base(database)
    {
    }

    protected override void Run(List<QueuedEvent> queue, ISearchIndex index)
    {
      CrawlingLog.Log.Debug(string.Format("[Index={0}] {1} executing.", index.Name, this.GetType().Name));

      if (this.Database == null)
      {
        CrawlingLog.Log.Fatal(string.Format("[Index={0}] OperationMonitor has invalid parameters. Index Update cancelled.", index.Name));
        return;
      }

      queue = queue.Where(q => q.Timestamp > (index.Summary.LastUpdatedTimestamp ?? 0)).ToList();

      if (queue.Count <= 0)
      {
        CrawlingLog.Log.Debug(string.Format("[Index={0}] Event Queue is empty. Incremental update returns", index.Name));
        return;
      }

      if (this.CheckForThreshold && queue.Count > this.ContentSearchSettings.FullRebuildItemCountThreshold())
      {
        CrawlingLog.Log.Warn(string.Format("[Index={0}] The number of changes exceeded maximum threshold of '{1}'.", index.Name, this.ContentSearchSettings.FullRebuildItemCountThreshold()));
        if (this.RaiseRemoteEvents)
        {
          Job fullRebuildJob = IndexCustodian.FullRebuild(index);
          fullRebuildJob.Wait();
        }
        else
        {
          Job fullRebuildRemoteJob = IndexCustodian.FullRebuildRemote(index);
          fullRebuildRemoteJob.Wait();
        }

        return;
      }

      List<IndexableInfo> parsed = this.ExtractIndexableInfoFromQueue(queue).ToList();
      CrawlingLog.Log.Info(string.Format("[Index={0}] Updating '{1}' items from Event Queue.", index.Name, parsed.Count()));

      Job incrememtalUpdateJob = IndexCustodian.IncrementalUpdate(index, parsed);
      incrememtalUpdateJob.Wait();
    }


    protected new IEnumerable<IndexableInfo> ExtractIndexableInfoFromQueue(List<QueuedEvent> queue)
    {
      var indexableListToUpdate = new Dictionary<DataUri, IndexableInfo>();
      var indexableListToRemove = new Dictionary<DataUri, IndexableInfo>();
      var indexableListToAddVersion = new Dictionary<DataUri, IndexableInfo>();

      foreach (var queuedEvent in queue)
      {
        var instanceData = this.Database.RemoteEvents.Queue.DeserializeEvent(queuedEvent) as ItemRemoteEventBase;

        if (instanceData == null)
        {
          continue;
        }

        var key = new DataUri(ID.Parse(instanceData.ItemId), Language.Parse(instanceData.LanguageName), Data.Version.Parse(instanceData.VersionNumber));
        var itemUri = new ItemUri(key.ItemID, key.Language, key.Version, this.Database);
        var indexable = new IndexableInfo(new SitecoreItemUniqueId(itemUri), queuedEvent.Timestamp);

        if (instanceData is RemovedVersionRemoteEvent || instanceData is DeletedItemRemoteEvent)
        {
          this.HandleIndexableToRemove(indexableListToRemove, key, indexable);
        }
        else if (instanceData is AddedVersionRemoteEvent)
        {
          this.HandleIndexableToAddVersion(indexableListToAddVersion, key, indexable);
        }
        else
        {
          this.UpdateIndexableInfo(instanceData, indexable);
          this.HandleIndexableToUpdate(indexableListToUpdate, key, indexable);
        }
      }

      return indexableListToUpdate.Select(x => x.Value)
        .Union(indexableListToRemove.Select(x => x.Value))
        .Union(indexableListToAddVersion.Select(x => x.Value))
        .OrderBy(x => x.Timestamp).ToList();
    }

    private void UpdateIndexableInfo(ItemRemoteEventBase instanceData, IndexableInfo indexable)
    {
      if (instanceData is SavedItemRemoteEvent)
      {
        var savedEvent = instanceData as SavedItemRemoteEvent;

        if (savedEvent.IsSharedFieldChanged)
        {
          indexable.IsSharedFieldChanged = true;
        }

        if (savedEvent.IsUnversionedFieldChanged)
        {
          indexable.IsUnversionedFieldChanged = true;
        }
      }

      if (instanceData is RestoreItemCompletedEvent)
      {
        indexable.IsSharedFieldChanged = true;
      }

      if (instanceData is CopiedItemRemoteEvent)
      {
        indexable.IsSharedFieldChanged = true;
        var copiedItemData = instanceData as CopiedItemRemoteEvent;
        if (copiedItemData.Deep)
        {
          indexable.NeedUpdateChildren = true;
        }
      }

      var @event = instanceData as MovedItemRemoteEvent;
      if (@event != null)
      {
        indexable.NeedUpdateChildren = true;
        indexable.OldParentId = @event.OldParentId;
      }
    }


    private void HandleIndexableToRemove(Dictionary<DataUri, IndexableInfo> collection, DataUri key, IndexableInfo indexable)
    {
      if (collection.ContainsKey(key))
      {
        collection[key].Timestamp = indexable.Timestamp;
      }
      else
      {
        collection.Add(key, indexable);
      }
    }

    private void HandleIndexableToAddVersion(Dictionary<DataUri, IndexableInfo> collection, DataUri key, IndexableInfo indexable)
    {
      indexable.IsVersionAdded = true;
      if (!collection.ContainsKey(key))
      {
        collection.Add(key, indexable);
      }
    }

    private void HandleIndexableToUpdate(Dictionary<DataUri, IndexableInfo> collection, DataUri key, IndexableInfo indexable)
    {
      bool alreadySetNeedUpdateChildren = collection.Any(x => x.Key.ItemID == key.ItemID && x.Value.NeedUpdateChildren);
      bool alreadyAddedSharedFieldChange = collection.Any(x => x.Key.ItemID == key.ItemID && x.Value.IsSharedFieldChanged);
      bool alreadyAddedUnversionedFieldChange = collection.Any(x => x.Key.ItemID == key.ItemID && x.Key.Language == key.Language && x.Value.IsUnversionedFieldChanged);

      #region Sitecore.Support.162291.166467
      if (alreadySetNeedUpdateChildren || alreadyAddedSharedFieldChange)
      {
        IndexableInfo value = null;
        try
        {
          value = collection.First((KeyValuePair<DataUri, IndexableInfo> x) => x.Key.ItemID == key.ItemID && x.Key.Language == key.Language && x.Key.Version.Number == key.Version.Number).Value;
        }
        catch
        {
          value = null;
        }

        if (value != null)
        {
          value.Timestamp = indexable.Timestamp;
          value.NeedUpdateChildren = value.NeedUpdateChildren || indexable.NeedUpdateChildren;
        }
        else
        {
          collection.Keys.Where(x => x.ItemID == key.ItemID && x.Language == key.Language).ToList().ForEach(x => collection.Remove(x));

          indexable.NeedUpdateChildren = (alreadySetNeedUpdateChildren || indexable.NeedUpdateChildren);
          collection.Add(key, indexable);
        }
      }
      //if (alreadySetNeedUpdateChildren || alreadyAddedSharedFieldChange)
      //{
      //  var entry = collection.First(x => x.Key.ItemID == key.ItemID).Value;
      //  entry.Timestamp = indexable.Timestamp;
      //  entry.NeedUpdateChildren = entry.NeedUpdateChildren || indexable.NeedUpdateChildren;
      //}
      #endregion

      else if (indexable.IsSharedFieldChanged || indexable.NeedUpdateChildren)
      {
        collection.Keys.Where(x => x.ItemID == key.ItemID).ToList().ForEach(x => collection.Remove(x));
        collection.Add(key, indexable);
      }
      else if (alreadyAddedUnversionedFieldChange)
      {
        collection.First(x => x.Key.ItemID == key.ItemID && x.Key.Language == key.Language).Value.Timestamp = indexable.Timestamp;
      }
      else if (indexable.IsUnversionedFieldChanged)
      {
        collection.Keys.Where(x => x.ItemID == key.ItemID && x.Language == key.Language).ToList().ForEach(x => collection.Remove(x));
        collection.Add(key, indexable);
      }
      else
      {
        if (collection.ContainsKey(key))
        {
          collection[key].Timestamp = indexable.Timestamp;
        }
        else
        {
          collection.Add(key, indexable);
        }
      }
    }
  }
}