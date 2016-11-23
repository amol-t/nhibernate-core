using System;
using System.Collections;
using System.Threading;
using System.Transactions;
using NHibernate.Engine;
using NHibernate.Engine.Transaction;
using NHibernate.Impl;

namespace NHibernate.Transaction
{
	public class AdoNetWithDistributedTransactionFactory : ITransactionFactory
	{
		private static readonly IInternalLogger logger = LoggerProvider.LoggerFor(typeof(ITransactionFactory));

		private readonly AdoNetTransactionFactory adoNetTransactionFactory = new AdoNetTransactionFactory();

		public void Configure(IDictionary props)
		{
		}

		public ITransaction CreateTransaction(ISessionImplementor session)
		{
			return new AdoTransaction(session);
		}

		public void EnlistInDistributedTransactionIfNeeded(ISessionImplementor session)
		{
			if (session.TransactionContext != null)
				return;
			
			if (System.Transactions.Transaction.Current == null)
				return;

			new DistributedTransactionContext(session, System.Transactions.Transaction.Current);
		}

		public bool IsInDistributedActiveTransaction(ISessionImplementor session)
		{
			var distributedTransactionContext = (DistributedTransactionContext)session.TransactionContext;
		    return distributedTransactionContext != null;
		}

		public void ExecuteWorkInIsolation(ISessionImplementor session, IIsolatedWork work, bool transacted)
		{
			using (var tx = new TransactionScope(TransactionScopeOption.Suppress))
			{
				// instead of duplicating the logic, we suppress the DTC transaction and create
				// our own transaction instead
				adoNetTransactionFactory.ExecuteWorkInIsolation(session, work, transacted);
				tx.Complete();
			}
		}

		public class DistributedTransactionContext : ITransactionContext, IEnlistmentNotification
		{
		    public bool ShouldCloseSessionOnDistributedTransactionCompleted { get; set; }

		    System.Transactions.Transaction _transaction;
		    ISessionImplementor _session;

		    public DistributedTransactionContext(ISessionImplementor session, System.Transactions.Transaction transaction)
			{
                _session = session;
			    _session.TransactionContext = this;
				_transaction = transaction.Clone();
				logger.DebugFormat("enlisted into DTC transaction: {0}",
								   _transaction.IsolationLevel);
				_session.AfterTransactionBegin(null);
				_transaction.EnlistVolatile(this, EnlistmentOptions.EnlistDuringPrepareRequired);
			}

			void IEnlistmentNotification.Prepare(PreparingEnlistment preparingEnlistment)
			{
				using (new SessionIdLoggingContext(_session.SessionId))
				{
					try
					{
						using (var tx = new TransactionScope(_transaction))
						{
							_session.BeforeTransactionCompletion(null);
							if (_session.FlushMode != FlushMode.Never && _session.ConnectionManager.IsConnected)
							{
								using (_session.ConnectionManager.FlushingFromDtcTransaction)
								{
									logger.Debug(String.Format("[session-id={0}] Flushing from Dtc Transaction", _session.SessionId));
									_session.Flush();
								}
							}
							logger.Debug("prepared for DTC transaction");

							tx.Complete();
						}
						preparingEnlistment.Prepared();
					}
					catch (Exception exception)
					{
						logger.Error("DTC transaction prepare phase failed", exception);
						OnTransactionCompletion(false);
						preparingEnlistment.ForceRollback(exception);
					}
				}
			}

			void IEnlistmentNotification.Commit(Enlistment enlistment)
			{
				using (new SessionIdLoggingContext(_session.SessionId))
				{
					logger.Debug("committing DTC transaction");
				    OnTransactionCompletion(true);
				}
				enlistment.Done();
			}

			void IEnlistmentNotification.Rollback(Enlistment enlistment)
			{
				using (new SessionIdLoggingContext(_session.SessionId))
				{
					logger.Debug("rolled back DTC transaction");
					OnTransactionCompletion(false);
				}
				enlistment.Done();
			}

			void IEnlistmentNotification.InDoubt(Enlistment enlistment)
			{
				using (new SessionIdLoggingContext(_session.SessionId))
				{
					logger.Debug("DTC transaction is in doubt");
					OnTransactionCompletion(false);
				}
				enlistment.Done();
			}

			public void Dispose()
			{
				if (_transaction != null)
					_transaction.Dispose();
			    _transaction = null;
			    if (_session != null)
			        _session.TransactionContext = null;
			    _session = null;
            }

            void OnTransactionCompletion(bool wasSuccessful)
			{
				using (new SessionIdLoggingContext(_session.SessionId))
				{
				    _session.TransactionContext = null;
                    _session.AfterTransactionCompletion(wasSuccessful, null);
				    if (ShouldCloseSessionOnDistributedTransactionCompleted)
				    {
				        _session.CloseSessionFromDistributedTransaction();
				    }
				    Dispose();
				}
            }
		}
	}
}