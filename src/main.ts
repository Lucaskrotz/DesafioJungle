import { createApp } from './bootstrap';
import { loadConfig } from './config';

const app = await createApp();
app.enableShutdownHooks();
await app.listen(loadConfig().port);
