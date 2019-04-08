import { createProjector } from 'maquette';
import { render as renderApplication } from './application';

createProjector({}).append(document.body, renderApplication);
