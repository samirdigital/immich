import { WorkflowTrigger } from '@immich/plugin-sdk';
import { WorkflowResult, WorkflowType } from 'src/enum.js';
import { WorkflowExecutionService } from 'src/services/workflow-execution.service.js';
import { newTestService } from 'test/utils.js';

it('retains fresh run UUIDs and optional workflow logging for context-free jobs', async () => {
  const { sut, mocks } = newTestService(WorkflowExecutionService);
  mocks.workflow.getForWorkflowRun.mockResolvedValue({
    id: 'workflow',
    name: 'local',
    trigger: WorkflowTrigger.AssetCreate,
    logging: true,
    steps: [
      {
        id: 'step',
        pluginId: 'plugin',
        methodName: 'process',
        types: [WorkflowType.AssetV1],
        hostFunctions: false,
        allowedHosts: [],
        config: null,
      },
    ],
  } as any);
  mocks.workflow.getForAssetV1.mockResolvedValue({ id: 'asset', ownerId: 'owner' } as any);
  mocks.plugin.callMethod.mockResolvedValue({});
  mocks.crypto.signJwt.mockReturnValue('local-token');
  mocks.workflow.log.mockResolvedValue([]);
  await sut.handleAssetTrigger({ workflowId: 'workflow', assetId: 'asset' });
  await sut.handleAssetTrigger({ workflowId: 'workflow', assetId: 'asset' });
  const logs = mocks.workflow.log.mock.calls.map(([row]) => row);
  expect(logs).toHaveLength(2);
  expect(logs[0]).toMatchObject({ result: WorkflowResult.Completed });
  expect(logs[0].runId).not.toBe(logs[1].runId);
});

it('retains local generic fanout and suppresses cloud publication generic fanout', async () => {
  const { sut, mocks } = newTestService(WorkflowExecutionService);
  mocks.workflow.search.mockResolvedValue([{ id: 'local-workflow' }] as any);
  mocks.job.queueAll.mockResolvedValue(undefined);
  await sut.onAssetCreate({ asset: { id: 'asset', ownerId: 'owner' } });
  expect(mocks.workflow.search).toHaveBeenCalledTimes(1);
  await sut.onAssetCreate({
    asset: { id: 'asset', ownerId: 'owner' },
    publication: { effectId: 'effect', receiptId: 'receipt', assetId: 'asset', ownerId: 'owner' },
  });
  expect(mocks.workflow.search).toHaveBeenCalledTimes(1);
});
