import { createApp } from './bootstrap';
import { loadConfig } from './config';

const config = loadConfig();
const app = await createApp({ workers: config.runWorkers });
app.enableShutdownHooks();
await app.listen(config.port);
