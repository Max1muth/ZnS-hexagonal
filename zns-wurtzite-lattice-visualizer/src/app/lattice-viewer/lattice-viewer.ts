import {
  Component,
  ElementRef,
  OnDestroy,
  ViewChild,
  AfterViewInit,
  HostListener,
  ChangeDetectionStrategy,
} from '@angular/core';
import { CommonModule } from '@angular/common';
import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js';

@Component({
  selector: 'app-lattice-viewer',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="relative w-full h-screen bg-[#020617] overflow-hidden font-sans text-slate-300">
      <!-- Header -->
      <header class="absolute top-8 left-8 z-10 max-w-lg pointer-events-none">
        <h1 class="text-4xl font-bold text-white mb-4">ZnS Wurtzite</h1>
        <p class="text-lg text-slate-400 leading-relaxed">
          Interactive 3D model of the hexagonal Zinc Sulfide crystal structure. 
          The Wurtzite structure consists of two interpenetrating hexagonal 
          close-packed lattices of Zinc and Sulfur.
        </p>
      </header>

      <!-- Three.js Canvas Container -->
      <div #canvasContainer class="w-full h-full cursor-move"></div>

      <!-- Legend (Bottom Left) -->
      <div class="absolute bottom-8 left-8 z-10 flex flex-col gap-3 pointer-events-none">
        <div class="bg-slate-900/40 backdrop-blur-sm border border-white/5 px-4 py-3 rounded-xl flex items-center gap-3 pointer-events-auto">
          <div class="w-4 h-4 rounded-full bg-[#facc15] shadow-[0_0_12px_rgba(250,204,21,0.4)]"></div>
          <span class="text-sm font-medium text-slate-200">Sulfur (S²⁻)</span>
        </div>
        <div class="bg-slate-900/40 backdrop-blur-sm border border-white/5 px-4 py-3 rounded-xl flex items-center gap-3 pointer-events-auto">
          <div class="w-4 h-4 rounded-full bg-[#94a3b8] shadow-[0_0_12px_rgba(148,163,184,0.4)]"></div>
          <span class="text-sm font-medium text-slate-200">Zinc (Zn²⁺)</span>
        </div>
      </div>

      <!-- Controls Hint (Bottom Right) -->
      <div class="absolute bottom-8 right-8 z-10 pointer-events-none">
        <div class="bg-slate-900/40 backdrop-blur-sm border border-white/5 px-6 py-3 rounded-xl pointer-events-auto">
          <span class="text-sm text-slate-400">Drag to rotate • Scroll to zoom</span>
        </div>
      </div>
    </div>
  `,
  styles: [`
    :host {
      display: block;
      width: 100%;
      height: 100%;
    }
  `],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class LatticeViewer implements AfterViewInit, OnDestroy {
  @ViewChild('canvasContainer') canvasContainer!: ElementRef<HTMLDivElement>;

  private scene!: THREE.Scene;
  private camera!: THREE.PerspectiveCamera;
  private renderer!: THREE.WebGLRenderer;
  private controls!: OrbitControls;
  private frameId: number | null = null;
  
  private atomsGroup = new THREE.Group();
  private bondsGroup = new THREE.Group();
  private latticeGroup = new THREE.Group();

  // Lattice constants for ZnS (Wurtzite)
  private readonly a = 3.82;
  private readonly c = 6.26;

  ngAfterViewInit() {
    this.initThree();
    this.createLattice();
    this.animate();
  }

  ngOnDestroy() {
    if (this.frameId) {
      cancelAnimationFrame(this.frameId);
    }
    this.renderer.dispose();
  }

  @HostListener('window:resize')
  onResize() {
    const width = window.innerWidth;
    const height = window.innerHeight;

    this.camera.aspect = width / height;
    this.camera.updateProjectionMatrix();
    this.renderer.setSize(width, height);
  }

  private initThree() {
    this.scene = new THREE.Scene();
    this.scene.background = new THREE.Color(0x020617);

    const width = window.innerWidth;
    const height = window.innerHeight;

    this.camera = new THREE.PerspectiveCamera(40, width / height, 0.1, 1000);
    this.camera.position.set(12, 8, 12);

    this.renderer = new THREE.WebGLRenderer({ 
      antialias: true,
    });
    this.renderer.setSize(width, height);
    this.renderer.setPixelRatio(window.devicePixelRatio);
    this.canvasContainer.nativeElement.appendChild(this.renderer.domElement);

    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.enableDamping = true;
    this.controls.dampingFactor = 0.05;
    this.controls.rotateSpeed = 0.8;

    // Lights
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.4);
    this.scene.add(ambientLight);

    const mainLight = new THREE.DirectionalLight(0xffffff, 1.2);
    mainLight.position.set(10, 20, 15);
    this.scene.add(mainLight);

    const fillLight = new THREE.PointLight(0x3b82f6, 0.5, 50);
    fillLight.position.set(-10, -5, -10);
    this.scene.add(fillLight);

    this.scene.add(this.latticeGroup);
    this.latticeGroup.add(this.atomsGroup);
    this.latticeGroup.add(this.bondsGroup);
  }

  private createLattice() {
    // Basis vectors for hexagonal lattice
    const a1 = new THREE.Vector3(this.a, 0, 0);
    const a2 = new THREE.Vector3(-this.a / 2, (this.a * Math.sqrt(3)) / 2, 0);
    const a3 = new THREE.Vector3(0, 0, this.c);

    // Atom materials
    const sMaterial = new THREE.MeshPhongMaterial({ 
      color: 0xfacc15, 
      shininess: 80,
      specular: 0x222222
    });
    const znMaterial = new THREE.MeshPhongMaterial({ 
      color: 0x94a3b8, 
      shininess: 80,
      specular: 0x222222
    });

    const sGeometry = new THREE.SphereGeometry(0.4, 32, 32);
    const znGeometry = new THREE.SphereGeometry(0.35, 32, 32);

    const atoms: { pos: THREE.Vector3, type: 'Zn' | 'S' }[] = [];

    // Create a larger grid of unit cells (e.g., 3x3x2)
    const rangeX = [-1, 0, 1];
    const rangeY = [-1, 0, 1];
    const rangeZ = [0, 1];

    const lineMaterial = new THREE.LineBasicMaterial({ color: 0x334155, transparent: true, opacity: 0.3 });

    for (const i of rangeX) {
      for (const j of rangeY) {
        for (const k of rangeZ) {
          const cellOrigin = new THREE.Vector3()
            .addScaledVector(a1, i)
            .addScaledVector(a2, j)
            .addScaledVector(a3, k);

          // Basis positions in fractional coordinates for Wurtzite
          const basis = [
            { pos: new THREE.Vector3(0, 0, 0), type: 'Zn' as const },
            { pos: new THREE.Vector3(2/3, 1/3, 1/2), type: 'Zn' as const },
            { pos: new THREE.Vector3(0, 0, 3/8), type: 'S' as const },
            { pos: new THREE.Vector3(2/3, 1/3, 7/8), type: 'S' as const }
          ];

          basis.forEach(b => {
            const worldPos = new THREE.Vector3()
              .copy(cellOrigin)
              .addScaledVector(a1, b.pos.x)
              .addScaledVector(a2, b.pos.y)
              .addScaledVector(a3, b.pos.z);
            
            if (!atoms.some(a => a.pos.distanceTo(worldPos) < 0.1)) {
              atoms.push({ pos: worldPos, type: b.type });
              const mesh = new THREE.Mesh(
                b.type === 'S' ? sGeometry : znGeometry,
                b.type === 'S' ? sMaterial : znMaterial
              );
              mesh.position.copy(worldPos);
              this.atomsGroup.add(mesh);
            }
          });

          // Draw wireframe for each unit cell
          const cellEdges = [
            [cellOrigin, cellOrigin.clone().add(a1)],
            [cellOrigin.clone().add(a1), cellOrigin.clone().add(a1).add(a2)],
            [cellOrigin.clone().add(a1).add(a2), cellOrigin.clone().add(a2)],
            [cellOrigin.clone().add(a2), cellOrigin],
            
            [cellOrigin.clone().add(a3), cellOrigin.clone().add(a1).add(a3)],
            [cellOrigin.clone().add(a1).add(a3), cellOrigin.clone().add(a1).add(a2).add(a3)],
            [cellOrigin.clone().add(a1).add(a2).add(a3), cellOrigin.clone().add(a2).add(a3)],
            [cellOrigin.clone().add(a2).add(a3), cellOrigin.clone().add(a3)],

            [cellOrigin, cellOrigin.clone().add(a3)],
            [cellOrigin.clone().add(a1), cellOrigin.clone().add(a1).add(a3)],
            [cellOrigin.clone().add(a1).add(a2), cellOrigin.clone().add(a1).add(a2).add(a3)],
            [cellOrigin.clone().add(a2), cellOrigin.clone().add(a2).add(a3)]
          ];

          cellEdges.forEach(edge => {
            const geometry = new THREE.BufferGeometry().setFromPoints([edge[0], edge[1]]);
            const line = new THREE.Line(geometry, lineMaterial);
            this.latticeGroup.add(line);
          });
        }
      }
    }

    // Create Bonds
    const bondMaterial = new THREE.MeshPhongMaterial({ 
      color: 0x1e293b, 
      shininess: 10
    });

    atoms.forEach((atomA, idxA) => {
      atoms.forEach((atomB, idxB) => {
        if (idxA >= idxB) return;
        if (atomA.type === atomB.type) return;

        const dist = atomA.pos.distanceTo(atomB.pos);
        if (dist > 2.2 && dist < 2.5) {
          this.createBond(atomA.pos, atomB.pos, bondMaterial);
        }
      });
    });

    // Center the whole group
    const box = new THREE.Box3().setFromObject(this.latticeGroup);
    const center = new THREE.Vector3();
    box.getCenter(center);
    this.latticeGroup.position.sub(center);
  }

  private createBond(start: THREE.Vector3, end: THREE.Vector3, material: THREE.Material) {
    const direction = new THREE.Vector3().subVectors(end, start);
    const length = direction.length();
    
    const geometry = new THREE.CylinderGeometry(0.12, 0.12, length, 8);
    const mesh = new THREE.Mesh(geometry, material);
    
    const midpoint = new THREE.Vector3().addVectors(start, end).multiplyScalar(0.5);
    mesh.position.copy(midpoint);
    
    const axis = new THREE.Vector3(0, 1, 0);
    mesh.quaternion.setFromUnitVectors(axis, direction.clone().normalize());
    
    this.bondsGroup.add(mesh);
  }

  private animate() {
    this.frameId = requestAnimationFrame(() => this.animate());
    this.controls.update();
    this.renderer.render(this.scene, this.camera);
  }
}
